package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.Message {
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
	public static class TaskMessage implements Message, LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -4667745204456518160L;
		List<String> batch;
		int fileShift;
		int hashAreaId;
		int columnId;
		ActorRef<DependencyMiner.Message> dependencyMiner;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class SendResultsMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int numColumns;
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
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private int hashAreaId = -1;

	private HashMap<String, BigInteger> hashMap;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(SendResultsMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");

		int hashAreaId = message.getHashAreaId();
		int columnId = message.getColumnId();
		int fileShift = message.getFileShift();
		if(this.hashAreaId != hashAreaId)
			this.hashMap = new HashMap<>();
		this.hashAreaId = hashAreaId;

		List<String> batch = message.getBatch();
		BigInteger representation = BigInteger.ONE.shiftLeft(fileShift + columnId);

		for(String s : batch){
			this.hashMap.merge(s, representation, (rep1,rep2) -> rep1.or(rep2));
		}

		message.getDependencyMiner().tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), hashAreaId));
		return this;
	}

	private Behavior<Message> handle(SendResultsMessage message) {
		this.getContext().getLog().info("Sending results...");
		int numColumns = message.getNumColumns();
		boolean[][] result = new boolean[numColumns][numColumns];
		for(int i = 0; i < numColumns; i++){
			outerLoop:
			for(int j = 0; j < numColumns; j++){
				if(i == j) continue;
				for(BigInteger value:this.hashMap.values()){
					if(value.testBit(i) && !value.testBit(j))
						continue outerLoop;
				}
				result[i][j] = true; //Column i seems to be included in column j
			}
		}
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(new DependencyMiner.ResultMessage(this.getContext().getSelf(), this.hashAreaId, result), message.getDependencyMinerLargeMessageProxy()));
		return this;
	}
}
