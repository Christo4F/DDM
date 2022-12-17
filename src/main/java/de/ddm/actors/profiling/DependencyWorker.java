package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.CollectorBuffer;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
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
		ActorRef<DependencyMiner.Message> dependencyMiner;
		List<String[]> batch;
		int shift;
	}


	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CollectorRegistrationAckMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<CollectorBuffer.Message> collectorBuffer;
		int collectorRangeID;
	}

	@Getter
	@NoArgsConstructor
	public static class FinalizeMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create(final List<ActorRef<DependencyCollector.Message>> collectors) {
		return Behaviors.setup(context -> new DependencyWorker(context, collectors));
	}

	private DependencyWorker(ActorContext<Message> context, final List<ActorRef<DependencyCollector.Message>> collectors) {
		super(context);
		this.collectors = collectors;
		this.numCollectors = collectors.size();

		for(ActorRef<DependencyCollector.Message> collector:collectors){
			collector.tell(new DependencyCollector.WorkerRegistrationMessage(this.getContext().getSelf()));
		}

		this.collectorBuffers = new HashMap<Integer,ActorRef<CollectorBuffer.Message>>(numCollectors);

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final List<ActorRef<DependencyCollector.Message>> collectors;

	//private final List<ActorRef<CollectorBuffer.Message>> collectorBuffers;
	private final HashMap<Integer, ActorRef<CollectorBuffer.Message>> collectorBuffers;
	private final int numCollectors;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(CollectorRegistrationAckMessage.class, this::handle)
				.onMessage(FinalizeMessage.class, this::handle)
				.build();
	}

	//Communication with DependencyCollector
	private Behavior<Message> handle(CollectorRegistrationAckMessage message) {
		this.collectorBuffers.put(new Integer(message.collectorRangeID), message.getCollectorBuffer());
		this.getContext().getLog().info("Registered at Collector {}", message.getCollectorRangeID());
		for(int i = 0; i < this.numCollectors; i++){
			if(!this.collectorBuffers.containsKey(new Integer(i))){
				this.getContext().getLog().info("Missing Collector {}", i);
				return this;
			}
		}
		this.getContext().getLog().info("All Collectors registered. Searching for Miner...");
		//Propagate Existence to Dependency Miner
		final ActorRef<Receptionist.Listing> listingResponseAdapter = this.getContext().messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		this.getContext().getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));
		return this;
	}

	//Communication with Dependency Miner
	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");
		List<String[]> batch = message.getBatch();
		int shift = message.getShift();

		HashMap<String, BigInteger>[] hashMaps = new HashMap[this.numCollectors];
		for(int i = 0; i < hashMaps.length; i++)
			hashMaps[i] = new HashMap<>();

		for(String[] row : batch){
			for(int rowId = 0; rowId < row.length; rowId++){
				int collector = ((row[rowId].hashCode() % numCollectors) + numCollectors) % numCollectors;
				hashMaps[collector].merge(row[rowId], BigInteger.ONE.shiftLeft(shift + rowId), (value1, value2) -> value1.or(value2));
			}
		}

		for(int i = 0; i < this.numCollectors; i++){
			this.collectorBuffers.get(new Integer(i)).tell(new CollectorBuffer.HashMapMessage(hashMaps[i]));
		}
		message.getDependencyMiner().tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(FinalizeMessage message) {
		this.getContext().getLog().info("Finalize Message Receiverd!");
		for(ActorRef<DependencyCollector.Message> collector : this.collectors)
			collector.tell(new DependencyCollector.WorkerFinalizedMessage(this.getContext().getSelf()));
		return this;
	}
}
