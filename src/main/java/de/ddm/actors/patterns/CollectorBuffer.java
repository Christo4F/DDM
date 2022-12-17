package de.ddm.actors.patterns;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.profiling.DependencyCollector;
import de.ddm.actors.profiling.DependencyMerger;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class CollectorBuffer extends AbstractBehavior<CollectorBuffer.Message> {
    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @Getter
    @NoArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HashMapMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        HashMap<String, BigInteger> hashMap;
    }

    @Getter
    @NoArgsConstructor
    public static class FinalizeMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "collectorBuffer";



    public static Behavior<CollectorBuffer.Message> create(final ActorRef<DependencyCollector.Message> dependencyCollector) {
        return Behaviors.setup(context -> new CollectorBuffer(context, dependencyCollector));
    }

    private CollectorBuffer(ActorContext<CollectorBuffer.Message> context, final ActorRef<DependencyCollector.Message> dependencyCollector) {
        super(context);
        this.dependencyCollector = dependencyCollector;
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
        this.hashMapContainer = new ArrayList<HashMap<String, BigInteger>>();
    }

    ////////////////////////
    // Actor State //
    ////////////////////////

    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final ActorRef<DependencyCollector.Message> dependencyCollector;

    private final List<HashMap<String, BigInteger>> hashMapContainer;

    private boolean parentIdle = true;

    private boolean finalizeFlag = false;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<CollectorBuffer.Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(CompletionMessage.class, this::handle)
                .onMessage(HashMapMessage.class, this::handle)
                .onMessage(FinalizeMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(CompletionMessage message) {
        if(this.hashMapContainer.isEmpty()){
            if(this.finalizeFlag)
                this.dependencyCollector.tell(new DependencyCollector.CollectorFinalizedMessage());
            this.parentIdle = true;
            return this;
        }
        this.dependencyCollector.tell(new DependencyCollector.HashMapMessage(this.hashMapContainer.remove(0)));
        this.parentIdle = false;
        return this;
    }

    private Behavior<Message> handle(FinalizeMessage message) {
        this.getContext().getLog().info("Received Finalize Message");
        if(this.hashMapContainer.isEmpty()){
            this.dependencyCollector.tell(new DependencyCollector.CollectorFinalizedMessage());
            return this;
        }
        this.finalizeFlag = true;
        return this;
    }


    private Behavior<Message> handle(HashMapMessage message) {
        if(this.parentIdle){
            this.dependencyCollector.tell(new DependencyCollector.HashMapMessage(message.getHashMap()));
            this.parentIdle = false;
            return this;
        }

        this.hashMapContainer.add(message.getHashMap());
        if(this.hashMapContainer.size() > 10)
            this.getContext().getLog().info("Container has size " + this.hashMapContainer.size());
        return this;
    }

}
