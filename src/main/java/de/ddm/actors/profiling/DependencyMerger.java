package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DependencyMerger extends AbstractBehavior<DependencyMerger.Message> {
    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationMessage implements DependencyMerger.Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyCollector.Message> dependencyCollector;
        int collectorRangeID;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HashMapMessage implements DependencyMerger.Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyCollector.Message> dependencyCollector;
        HashMap<String, BigInteger> hashMap;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FinalizeMessage implements DependencyMerger.Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyCollector.Message> dependencyCollector;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyMerger";

    public static final ServiceKey<DependencyMerger.Message> dependencyMergerService = ServiceKey.create(DependencyMerger.Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create(final int mergerRangeID, final ActorRef<DependencyMiner.Message> dependencyMiner, final int numColumns) {
        return Behaviors.setup(context -> new DependencyMerger(context, mergerRangeID, dependencyMiner, numColumns));
    }

    private DependencyMerger(ActorContext<Message> context, final int mergerRangeID, final ActorRef<DependencyMiner.Message> dependencyMiner, final int numColumns) {
        super(context);
        this.mergerRangeID = mergerRangeID;
        this.dependencyCollectors = new ArrayList<ActorRef<DependencyCollector.Message>>();
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
        this.hashMap = new HashMap<String, BigInteger>();
        this.dependencyMiner = dependencyMiner;
        this.numColumns = numColumns;
        context.getSystem().receptionist().tell(Receptionist.register(dependencyMergerService, context.getSelf()));
    }

    ////////////////////////
    // Actor State //
    ////////////////////////

    private final List<ActorRef<DependencyCollector.Message>> dependencyCollectors;
    private final int mergerRangeID;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final HashMap<String, BigInteger> hashMap;

    private final ActorRef<DependencyMiner.Message> dependencyMiner;
    private final int numColumns;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<DependencyMerger.Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(RegistrationMessage.class, this::handle)
                .onMessage(HashMapMessage.class, this::handle)
                .build();
    }

    private Behavior<DependencyMerger.Message> handle(RegistrationMessage message) {
        ActorRef<DependencyCollector.Message> dependencyCollector = message.getDependencyCollector();
        int collectorRangeID = message.getCollectorRangeID();
        if(this.mergerRangeID != collectorRangeID)
            return this;
        if (!this.dependencyCollectors.contains(dependencyCollector)) {
            this.dependencyCollectors.add(dependencyCollector);
            this.getContext().watch(dependencyCollector);
            dependencyCollector.tell(new DependencyCollector.RegistrationAckMessage(this.largeMessageProxy, this.mergerRangeID));
        }
        return this;
    }

    private Behavior<DependencyMerger.Message> handle(HashMapMessage message) {
        ActorRef<DependencyCollector.Message> dependencyCollector = message.getDependencyCollector();
        HashMap<String, BigInteger> collectorHashMap = message.getHashMap();
        if(collectorHashMap.size() == 0){
            this.finalize(dependencyCollector);
            return this;
        }
        for (String key:collectorHashMap.keySet()) {
            this.hashMap.merge(key, collectorHashMap.get(key), (value1,value2) -> value1.or(value2));
        }
        dependencyCollector.tell(new DependencyCollector.CompletionMessage());
        return this;
    }

    private void finalize (ActorRef<DependencyCollector.Message> dependencyCollector) {
        this.dependencyCollectors.remove(dependencyCollector);
        if(!this.dependencyCollectors.isEmpty())
            return;

        //Compute which INGs can't exist
        //TODO find better algorithm for this
        boolean[][] result = new boolean[this.numColumns][this.numColumns];
        for(int i = 0; i < this.numColumns; i++){
            outerLoop:
            for(int j = 0; j < this.numColumns; j++){
                if(i == j) continue;
                for(BigInteger value:this.hashMap.values()){
                    if(value.testBit(i) && !value.testBit(j))
                        continue outerLoop;
                }
                result[i][j] = true; //Column i seems to be included in column j
            }
        }
        this.dependencyMiner.tell(new DependencyMiner.MergerResultMessage(this.getContext().getSelf(),  result));
        return;
    }



}
