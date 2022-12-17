package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.patterns.CollectorBuffer;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.DomainConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.*;

public class DependencyCollector extends AbstractBehavior<DependencyCollector.Message>{

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
    public static class HashMapMessage implements Message {
        private static final long serialVersionUID = -1963913284517850454L;
        HashMap<String, BigInteger> hashMap;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationAckMessage implements Message {
        private static final long serialVersionUID = -1963913284517850454L;
        ActorRef<LargeMessageProxy.Message> largeMessageProxy;
        int mergerRangeID;
    }

    @Getter
    @NoArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -1963913284517850454L;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkerRegistrationMessage implements Message {
        private static final long serialVersionUID = -1963913284517850454L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkerFinalizedMessage implements Message {
        private static final long serialVersionUID = -1963913284517850454L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
    }

    @Getter
    @NoArgsConstructor
    public static class CollectorFinalizedMessage implements Message {
        private static final long serialVersionUID = -1963913284517850454L;
    }





    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyCollector";

    public static Behavior<Message> create(final int collectorRangeId) {
        return Behaviors.setup(context -> new DependencyCollector(context, collectorRangeId));
    }

    private DependencyCollector(ActorContext<Message> context, final int collectorRangeID) {
        super(context);
        this.collectorRangeID = collectorRangeID;
        this.hashMap = new HashMap<String, BigInteger>();
        this.largeMessageProxy= this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
        this.buffer = this.getContext().spawn(CollectorBuffer.create(this.getContext().getSelf()), CollectorBuffer.DEFAULT_NAME);
        this.dependencyWorkers = new ArrayList<ActorRef<DependencyWorker.Message>>();

        final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMerger.dependencyMergerService, listingResponseAdapter));
    }

    /////////////////
    // Actor State //
    /////////////////
    private ActorRef<LargeMessageProxy.Message> dependencyMergerProxy;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private final ActorRef<CollectorBuffer.Message> buffer;
    private final HashMap<String, BigInteger> hashMap;
    private final int collectorRangeID;

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

    private boolean finalizeFlag = false;


    ////////////////////
    // Actor Behavior //
    ////////////////////
    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(RegistrationAckMessage.class, this::handle)
                .onMessage(HashMapMessage.class, this::handle)
                .onMessage(WorkerRegistrationMessage.class, this::handle)
                .onMessage(WorkerFinalizedMessage.class, this::handle)
                .onMessage(CompletionMessage.class, this::handle)
                .onMessage(CollectorFinalizedMessage.class, this::handle)
                .build();
    }

    //Communication with CollectorBuffer
    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMerger.Message>> dependencyMergers = message.getListing().getServiceInstances(DependencyMerger.dependencyMergerService);
        for (ActorRef<DependencyMerger.Message> dependencyMerger : dependencyMergers)
            dependencyMerger.tell(new DependencyMerger.RegistrationMessage(this.getContext().getSelf(), this.collectorRangeID));
        return this;
    }

    private Behavior<Message> handle(HashMapMessage message) {
        HashMap<String, BigInteger> workerHashMap = message.getHashMap();
        for (String key:workerHashMap.keySet()) {
            this.hashMap.merge(key, workerHashMap.get(key), (value1,value2) -> value1.or(value2));
        }
        this.buffer.tell(new CollectorBuffer.CompletionMessage());
        return this;
    }

    //Communication with DependencyWorker
    private Behavior<Message> handle(WorkerRegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        this.dependencyWorkers.add(dependencyWorker);
        dependencyWorker.tell(new DependencyWorker.CollectorRegistrationAckMessage(this.buffer, this.collectorRangeID));

        return this;
    }

    private Behavior<Message> handle(WorkerFinalizedMessage message) {
        this.dependencyWorkers.remove(message.getDependencyWorker());
        if(this.dependencyWorkers.size() > 0)
            return this;

        this.buffer.tell(new CollectorBuffer.FinalizeMessage());
        return this;
    }

    private Behavior<Message> handle(CollectorFinalizedMessage message) {
        this.finalizeFlag = true;
        this.getContext().getLog().info("Finalize Flag Set!");
        if(this.dependencyMergerProxy == null)
            return this;

        sendHashBatch(this.dependencyMergerProxy);
        return this;
    }

    //Communication with DependencyMerger
    private Behavior<Message> handle(RegistrationAckMessage message) {
        this.dependencyMergerProxy = message.getLargeMessageProxy();
        if(this.finalizeFlag)
            sendHashBatch(this.dependencyMergerProxy);
        return this;
    }

    private Behavior<Message> handle(DependencyCollector.CompletionMessage message) {
        sendHashBatch(this.dependencyMergerProxy);
        return this;
    }

    private void sendHashBatch(ActorRef<LargeMessageProxy.Message> largeMessageProxy){
        this.getContext().getLog().info("Sending hashBatch...");
        int collectorBatchSize = 10000; //TODO Make this configurable
        HashMap<String, BigInteger> hashBatch = new HashMap<String, BigInteger>(collectorBatchSize);
        int i = 0;
        Iterator<HashMap.Entry<String, BigInteger>> it=this.hashMap.entrySet().iterator();
        while(it.hasNext()){
            i++;
            if(i > collectorBatchSize)
                break;
            HashMap.Entry<String, BigInteger> e = it.next();
            hashBatch.put(e.getKey(), e.getValue());
            it.remove();
        }
        DependencyMerger.HashMapMessage message = new DependencyMerger.HashMapMessage(this.getContext().getSelf(), hashBatch);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(message, this.dependencyMergerProxy));
    }
}
