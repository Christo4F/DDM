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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

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
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int result;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class SortCompletionMessage implements Message{
		//private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		HashMap<Integer, List<String>> sortedColumnContainer;
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

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();


		this.batchMessages = new ArrayList<>();
		this.readInputFiles = new ArrayList<>();
		this.unSortedColumns = new ArrayList<>();
		this.unsortedIds = new ArrayList<>();
		this.finishedSortedListIds = new ArrayList<>();
		this.sortedColumnContainer = new HashMap<>();

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

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

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
				.onMessage(SortCompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	ArrayList<BatchMessage> batchMessages;
	ArrayList<Integer> readInputFiles;
	List<ArrayList<String>> unSortedColumns;
	List<Integer> unsortedIds;
	int idCounter = 0;
	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.

		this.getContext().getLog().info("File NR.{} and batches size {}", message.id, message.batch.size());
		//only adding column that hasn't been added before
		if (readInputFiles.size() < inputFiles.length){
			if (!readInputFiles.contains(message.id)) {
				readInputFiles.add(message.id);
				for (int i = 0; i < message.getBatch().get(0).length; i++){
					ArrayList<String> column = new ArrayList<>();
					for (int j = 0; j < message.getBatch().size(); j++) {
						String columnValue = message.getBatch().get(j)[i];
						column.add(columnValue);
					}
					unSortedColumns.add(column);
					unsortedIds.add(idCounter);
					idCounter += 1;
				}
				//storing Batchmessage
				batchMessages.add(message);
			}
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		} else {
		}
		return this;
	}

	int counter = 0;
	List<Integer> finishedSortedListIds;
	private void handSortTaskToWorker(ActorRef<DependencyWorker.Message> dependencyWorker) {
		//if (counter <= unsortedIds.size()) {
		if (!unSortedColumns.isEmpty()) {
			List<String> column = unSortedColumns.get(0);
			int columnId = unsortedIds.get(0);

			//remove handled id's from Array
			unSortedColumns.remove(column);
			unsortedIds.remove(Integer.valueOf(columnId));

			dependencyWorker.tell(new DependencyWorker.SortTaskMessage(this.largeMessageProxy, column, columnId));
		} else {
			handCompareTaskToWorker(dependencyWorker);
			getContext().getLog().info("HandSortTaskToWorker finished work!");
		}
	}

	int firstColumnCounter = 0;
	int secondColumnCounter = 0;
	private void handCompareTaskToWorker(ActorRef<DependencyWorker.Message> dependencyWorker) {
		List<List<String>> sortedColumns = sortedColumnContainer.values().stream().toList();

		List<String> firstColumn = sortedColumns.get(firstColumnCounter);
		List<String> secondColumn = sortedColumns.get(secondColumnCounter);
		int firstColumnId = (int) sortedColumnContainer.keySet().toArray()[firstColumnCounter];
		int secondColumnId = (int) sortedColumnContainer.keySet().toArray()[secondColumnCounter];

		secondColumnCounter++;
		if (secondColumnCounter >= sortedColumns.size()) {
			firstColumnCounter += 1;
			secondColumnCounter = firstColumnCounter;
		}
		dependencyWorker.tell(new DependencyWorker.TestMessage(this.largeMessageProxy, firstColumn, secondColumn, firstColumnId, secondColumnId));
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)

			handSortTaskToWorker(dependencyWorker);
			dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));
		}
		return this;
	}

	HashMap<Integer,List<String>> sortedColumnContainer;
	private Behavior<Message> handle(SortCompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		getContext().getLog().info("NOW I GET SOME WORK... i got {} things to do", message.sortedColumnContainer.size());
		sortedColumnContainer = message.getSortedColumnContainer();

		//jetzt sende immer jeweils 2 column inklusive id per TaskMessage an den Worker

		handCompareTaskToWorker(dependencyWorker);
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.

		if (this.headerLines[0] != null) {
			Random random = new Random();
			int dependent = random.nextInt(this.inputFiles.length);
			int referenced = random.nextInt(this.inputFiles.length);
			File dependentFile = this.inputFiles[dependent];
			File referencedFile = this.inputFiles[referenced];
			String[] dependentAttributes = {this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)], this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)]};
			String[] referencedAttributes = {this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)], this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)]};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(ind);

			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}
		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!
		handSortTaskToWorker(dependencyWorker);
		dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if (System.currentTimeMillis() - this.startTime > 2000000)
			this.end();
		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}