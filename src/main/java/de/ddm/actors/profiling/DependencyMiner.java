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
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.TableEntry;
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
		int hashAreaId;
		List<TableEntry> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> workerProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int hashAreaId;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ResultMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int hashAreaId;
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
		this.numHashAreas = SystemConfigurationSingleton.get().getNumHashAreas();
		this.unassignedTasksByHashAreaId = new List[this.numHashAreas];
		this.unassignedHashIds = new ArrayList<>();

		for(int i = 0; i < this.numHashAreas; i++){
			this.unassignedTasksByHashAreaId[i] = new ArrayList<>();
			this.unassignedHashIds.add(new Integer(i));
		}

		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.shifts = new int[this.inputFiles.length];
		this.headerReadDone = new boolean[this.inputFiles.length];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		this.inputReaderFinishedFlag = new boolean[inputFiles.length];
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id], this.numHashAreas), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.idleDependencyWorkers = new ArrayList<>();
		this.busyDependencyWorkers = new HashMap<>();
		this.waitingDependencyWorkers = new HashMap<>();
		this.workerProxys = new HashMap<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> idleDependencyWorkers;

	private final HashMap<Integer, ActorRef<DependencyWorker.Message>> busyDependencyWorkers;

	private final HashMap<Integer, ActorRef<DependencyWorker.Message>> waitingDependencyWorkers;

	private final HashMap<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> workerProxys;

	private final int numHashAreas;

	private final List<DependencyWorker.TaskMessage>[] unassignedTasksByHashAreaId;

	private final List<Integer> unassignedHashIds;

	private final boolean[] inputReaderFinishedFlag;

	private boolean inputFinishedFlag = false;

	private final boolean[] headerReadDone;

	private boolean[][] result;

	private int numColumns = 0;

	private final int[] shifts;

	////////////////////
	// Actor Behavior //
	////////////////////

	//TODO Use Autoboxing for new Integer(i) terms

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.onMessage(ResultMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		int id = message.getId();
		int shift = message.getHeader().length;
		this.numColumns += shift;

		for(int i = id + 1; i < this.shifts.length; i++){
			shifts[i] += shift;
		}

		this.headerReadDone[id] = true;
		this.getContext().getLog().info("Finished header reading of File {}", id);
		for(boolean b : this.headerReadDone)
			if(!b) return this;

		this.getContext().getLog().info("Finished all header readings.");


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

		List<TableEntry> batch = message.getBatch();
		int hashAreaId = message.getHashAreaId();
		int fileId = message.getId();

		if(batch == null){
			this.inputReaderFinishedFlag[fileId] = true;
			this.inputFinishedFlag = true;
			for(boolean b : this.inputReaderFinishedFlag)
				this.inputFinishedFlag = this.inputFinishedFlag && b;
			if(!this.inputFinishedFlag)
				return this;

			for(ActorRef<DependencyWorker.Message> waitingWorker : this.waitingDependencyWorkers.values()){
				waitingWorker.tell(new DependencyWorker.SendResultsMessage(this.largeMessageProxy, this.numColumns));
			}
			return this;
		}

		DependencyWorker.TaskMessage task = new DependencyWorker.TaskMessage(batch, this.shifts[fileId], hashAreaId, this.getContext().getSelf()); //TODO: FileShift

		if(this.waitingDependencyWorkers.containsKey(new Integer(hashAreaId))){
			ActorRef<DependencyWorker.Message> dependencyWorker = this.waitingDependencyWorkers.get(new Integer(hashAreaId));

			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task, this.workerProxys.get(dependencyWorker)));
			this.waitingDependencyWorkers.remove(dependencyWorker);
			this.busyDependencyWorkers.put(new Integer(hashAreaId), dependencyWorker);
		}
		else{
			this.unassignedTasksByHashAreaId[hashAreaId].add(task);
			this.getContext().getLog().info("Open Tasks on HashArea " + hashAreaId + " is " + this.unassignedTasksByHashAreaId[hashAreaId].size());
		}
		this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.waitingDependencyWorkers.values().contains(dependencyWorker) && !this.busyDependencyWorkers.values().contains(dependencyWorker) && !this.idleDependencyWorkers.contains(dependencyWorker)) {
			this.getContext().watch(dependencyWorker);
			this.workerProxys.put(dependencyWorker, message.getWorkerProxy());

			while(!this.unassignedHashIds.isEmpty()){
				int hashAreaId = this.unassignedHashIds.remove(0);
				if(this.unassignedTasksByHashAreaId[hashAreaId].isEmpty()){
					if(!this.inputFinishedFlag){
						this.waitingDependencyWorkers.put(new Integer(hashAreaId), dependencyWorker);
						return this;
					}
				}
				else{
					this.busyDependencyWorkers.put(new Integer(hashAreaId), dependencyWorker);
					DependencyWorker.TaskMessage task = this.unassignedTasksByHashAreaId[hashAreaId].remove(0);
					this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task, this.workerProxys.get(dependencyWorker)));
					return this;
				}
			}

			this.idleDependencyWorkers.add(dependencyWorker);
			return this;
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		int hashAreaId = message.getHashAreaId();

		if(!this.unassignedTasksByHashAreaId[hashAreaId].isEmpty()){
			DependencyWorker.TaskMessage task = this.unassignedTasksByHashAreaId[hashAreaId].remove(0);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task, this.workerProxys.get(dependencyWorker)));
			return this;
		}

		if(!this.inputFinishedFlag){
			this.busyDependencyWorkers.remove(new Integer(hashAreaId));
			this.waitingDependencyWorkers.put(new Integer(hashAreaId), dependencyWorker);
			return this;
		}

		dependencyWorker.tell(new DependencyWorker.SendResultsMessage(this.largeMessageProxy, this.numColumns));
		return this;

	}

	private Behavior<Message> handle(ResultMessage message) {
		boolean[][] workerResult = message.getResult();
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		int hashAreaId = message.getHashAreaId();

		for(int i = 0; i < this.numColumns; i++)
			for(int j = 0; j < this.numColumns; j++)
				this.result[i][j] = this.result[i][j] && workerResult[i][j];

		this.busyDependencyWorkers.remove(new Integer(hashAreaId));
		this.waitingDependencyWorkers.remove(new Integer(hashAreaId));

		while(!this.unassignedHashIds.isEmpty()){
			int newHashAreaId = this.unassignedHashIds.remove(0);
			if(this.unassignedTasksByHashAreaId[newHashAreaId].isEmpty())
				continue;

			this.busyDependencyWorkers.put(new Integer(newHashAreaId), dependencyWorker);
			DependencyWorker.TaskMessage task = this.unassignedTasksByHashAreaId[hashAreaId].remove(0);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(task, this.workerProxys.get(dependencyWorker)));
			return this;
		}

		this.idleDependencyWorkers.add(dependencyWorker);

		if(this.busyDependencyWorkers.isEmpty() && this.waitingDependencyWorkers.isEmpty()){
			this.end();
		}

		return this;
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

	private int findFileId(int columnId){
		for(int fileId = 0; fileId < this.inputFiles.length - 1; fileId++){
			if(this.shifts[fileId + 1] > columnId)
				return fileId;
		}
		return this.inputFiles.length - 1;
	}


	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.getContext().getLog().error("Unhandled DependencyWorker Termination.");
		return this;
	}
}