package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import com.sun.source.doctree.TextTree;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
		List<String> firstColumn;
		List<String> secondColumn;
		int firstColumnId;
		int secondColumnId;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class SortTaskMessage implements Message {
		//private static final long serialVersionUID???
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		List<String> column;
		int columnId;
		int numberOfColumns;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.sortedColumnContainer = new HashMap<>();

	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(SortTaskMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	HashMap<Integer,List<String>> sortedColumnContainer;
	int counter = 0;
	private Behavior<Message> handle(SortTaskMessage message) {

		List<String> unsortColumn = new ArrayList<>();
		List<String> sortedColumn = new ArrayList<>();
		int columnId = message.getColumnId();
		unsortColumn = message.getColumn();
		//Es sollen nur Anzahl columIds column sortiert werden. Wenn alle Columns sortiert sind, wird die Hashmap zurück gegeben.
		if (columnId < message.numberOfColumns) {

			getContext().getLog().info("Number of columns: " + message.numberOfColumns);
			//Sort column
			Collections.sort(unsortColumn);
			sortedColumn = unsortColumn;

			getContext().getLog().info("counter " + counter);
			//Save sorted Column by columnId

			//Erst später im DependencyMiner zusammenstellen
			sortedColumnContainer.put(columnId, sortedColumn);

			LargeMessageProxy.LargeMessage sortCompletionMessage = new DependencyMiner.SortCompletionMessage(this.getContext().getSelf(), sortedColumnContainer);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(sortCompletionMessage, message.getDependencyMinerLargeMessageProxy()));
			counter++;
		} else {
			getContext().getLog().info("Jetzt sende ich es an die SortCompletionMessage");
			LargeMessageProxy.LargeMessage sortCompletionMessage = new DependencyMiner.SortCompletionMessage(this.getContext().getSelf(), sortedColumnContainer);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(sortCompletionMessage, message.getDependencyMinerLargeMessageProxy()));
		}

		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		int firstColumnInSecondColumn = 0;
		int secondColumnInFirstColumn = 0;

		boolean firstInSecond;
		boolean secondInFirst;

		List<String> firstColumn = message.getFirstColumn();
		List<String> secondColumn = message.getSecondColumn();

		int firstColumnId = message.getFirstColumnId();
		int secondColumnId = message.getSecondColumnId();

		getContext().getLog().info("FirstColumn received " + Arrays.toString(firstColumn.toArray()));
		getContext().getLog().info("SecondColumn received " + Arrays.toString(secondColumn.toArray()));

		for (int i = 0; i < firstColumn.size(); i++) {
			for (int y = 0; y < secondColumn.size(); y++) {
				if (firstColumn.get(i).equals(secondColumn.get(y))) {
					firstColumnInSecondColumn++;
				} else if (secondColumn.get(y).equals(firstColumn.get(i))) {
					secondColumnInFirstColumn++;
				}
			}
		}

		if (firstColumnInSecondColumn == firstColumn.size()) {
			firstInSecond = true;
		} else {
			firstInSecond = false;
		}
		if (secondColumnInFirstColumn == secondColumn.size()) {
			secondInFirst = true;
		} else {
			secondInFirst = false;
		}

		//Hier steht noch der alte standard Code
		int result = message.getTask();
		long time = System.currentTimeMillis();
		Random rand = new Random();
		int runtime = (rand.nextInt(2) + 2) * 1000;
		while (System.currentTimeMillis() - time < runtime)
			result = ((int) Math.abs(Math.sqrt(result)) * result) % 1334525;

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result, firstColumnId, secondColumnId, firstInSecond, secondInFirst);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}
}
