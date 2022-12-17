package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class MergerResultMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyMerger.Message> dependencyMerger;
		boolean[][] result;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.shifts = new int[this.inputFiles.length];
		this.inputReaderDone = new boolean[this.inputFiles.length];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.idleDependencyWorkers = new ArrayList<>();
		this.busyDependencyWorkers = new ArrayList<>();
		this.workerProxyHash = new HashMap<>();
		this.taskContainer = new ArrayList<>();

		this.mergers = new ArrayList<>(this.numCollectors);


		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final int numCollectors = SystemConfigurationSingleton.get().getNumCollectors();

	private int numColumns;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private boolean finalizeFlag;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> idleDependencyWorkers;
	private final List<ActorRef<DependencyWorker.Message>> busyDependencyWorkers;

	private final HashMap<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> workerProxyHash;

	private final List<DependencyWorker.TaskMessage> taskContainer;

	private final List<ActorRef<DependencyMerger.Message>> mergers;

	private final int[] shifts;

	private final boolean[] inputReaderDone;

	private boolean[][] result;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(MergerResultMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		int id = message.getId();
		int shift = message.getHeader().length;
		this.numColumns += shift;
		for(int i = id + 1; i < this.shifts.length; i++){
			shifts[i] += shift;
		}
		this.headerLines[message.getId()] = message.getHeader();
		this.inputReaderDone[id] = true;
		this.getContext().getLog().info("Finished header reading of File {}", id);
		for(boolean b : this.inputReaderDone)
			if(!b) return this;
		this.getContext().getLog().info("Finished all header readings.", id);

		for (int i = 0; i < this.numCollectors; i++)
			this.mergers.add(this.getContext().spawn(DependencyMerger.create(i, this.getContext().getSelf(), this.numColumns), DependencyMerger.DEFAULT_NAME + "_" + i));

		this.result = new boolean[this.numColumns][this.numColumns];
		for(int i = 0; i < this.numColumns; i++)
			for(int j = 0; j < this.numColumns; j++)
				this.result[i][j] = true;

		for (ActorRef<InputReader.Message> inputReader : this.inputReaders) {
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		List<String[]> batch = message.getBatch();
		int id = message.getId();
		if(batch.size() == 0){
			this.inputReaderDone[id] = false;
			for(boolean b : this.inputReaderDone)
				if(b) return this;
			this.finalizeFlag = true;
			if(!this.taskContainer.isEmpty())
				return this;
			for(ActorRef<DependencyWorker.Message> worker: this.idleDependencyWorkers)
				worker.tell(new DependencyWorker.FinalizeMessage());
			for(ActorRef<DependencyWorker.Message> worker: this.busyDependencyWorkers)
				worker.tell(new DependencyWorker.FinalizeMessage());
			return this;
		}

		DependencyWorker.TaskMessage task = new DependencyWorker.TaskMessage(this.getContext().getSelf(), batch, this.shifts[id]);
		if(this.idleDependencyWorkers.isEmpty()) {
			this.taskContainer.add(task);
		}
		else{
			ActorRef<DependencyWorker.Message> dependencyWorker = this.idleDependencyWorkers.remove(0);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task,this.workerProxyHash.get(dependencyWorker)));
		}

		this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		ActorRef<LargeMessageProxy.Message> workerProxy = message.getLargeMessageProxy();
		if (!this.busyDependencyWorkers.contains(dependencyWorker) && !this.idleDependencyWorkers.contains(dependencyWorker)) {
			this.workerProxyHash.put(dependencyWorker, workerProxy);
			if(this.taskContainer.isEmpty())
				this.idleDependencyWorkers.add(dependencyWorker);
			else{
				this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(this.taskContainer.remove(0),workerProxy));
				this.busyDependencyWorkers.add(dependencyWorker);
			}
			this.getContext().watch(dependencyWorker);
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if(this.taskContainer.isEmpty()){
			this.busyDependencyWorkers.remove(dependencyWorker);
			this.idleDependencyWorkers.add(dependencyWorker);
			if(this.finalizeFlag){
				for(ActorRef<DependencyWorker.Message> worker: this.idleDependencyWorkers)
					worker.tell(new DependencyWorker.FinalizeMessage());
				for(ActorRef<DependencyWorker.Message> worker: this.busyDependencyWorkers)
					worker.tell(new DependencyWorker.FinalizeMessage());
			}
			return this;
		}
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(this.taskContainer.remove(0),this.workerProxyHash.get(dependencyWorker)));
		this.getContext().getLog().info("Open tasks: {}", this.taskContainer.size());
		return this;
	}

	private Behavior<Message> handle(MergerResultMessage message) {
		boolean[][] mergerResult = message.getResult();
		ActorRef<DependencyMerger.Message> dependencyMerger = message.getDependencyMerger();
		for(int i = 0; i < this.numColumns; i++)
			for(int j = 0; j < this.numColumns; j++)
				this.result[i][j] = this.result[i][j] && mergerResult[i][j];
		this.mergers.remove(dependencyMerger);
		if(!this.mergers.isEmpty())
			return this;

		this.end();

		return this;
	}

	private int findFileId(int columnId){
		for(int fileId = 0; fileId < this.inputFiles.length - 1; fileId++){
			if(this.shifts[fileId + 1] > columnId)
				return fileId;
		}
		return this.inputFiles.length - 1;
	}

	private void end() {
		List<InclusionDependency> dependencies = new ArrayList<>();
		for(int i = 0; i < this.numColumns; i++){
			for(int j = 0; j < this.numColumns; j++){
				if(i == j) continue;
				if(!this.result[i][j]) continue;
				int fileIdI = this.findFileId(i);
				int fileIdJ = this.findFileId(j);
				int columnIdI = i - this.shifts[fileIdI];
				int columnIdJ = j - this.shifts[fileIdJ];
				dependencies.add(new InclusionDependency(this.inputFiles[fileIdI], new String[]{this.headerLines[fileIdI][columnIdI]}, this.inputFiles[fileIdJ], new String[]{this.headerLines[fileIdJ][columnIdJ]}));
			}
		}
		this.resultCollector.tell(new ResultCollector.ResultMessage(dependencies));
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.getContext().getLog().error("Dependency Worker terminated, but no handling in Dependency Miner defined. Data may be lost.");
		return this;
	}
}