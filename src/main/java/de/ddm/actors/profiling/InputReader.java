package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.structures.TableEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InputReader extends AbstractBehavior<InputReader.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadHeaderMessage implements Message {
		private static final long serialVersionUID = 1729062814525657711L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadBatchMessage implements Message {
		private static final long serialVersionUID = -7915854043207237318L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "inputReader";

	public static Behavior<Message> create(final int id, final File inputFile, final int numHashAreas) {
		return Behaviors.setup(context -> new InputReader(context, id, inputFile, numHashAreas));
	}

	private InputReader(ActorContext<Message> context, final int id, final File inputFile, final int numHashAreas) throws IOException, CsvValidationException {
		super(context);
		this.id = id;
		this.reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
		this.header = InputConfigurationSingleton.get().getHeader(inputFile);
		this.numHashAreas = numHashAreas;

		this.entries = new List[numHashAreas];
		for(int i = 0; i < numHashAreas; i++)
			entries[i] = new ArrayList<>();

		if (InputConfigurationSingleton.get().isFileHasHeader())
			this.reader.readNext();
	}

	/////////////////
	// Actor State //
	/////////////////

	private final int id;
	private final int batchSize = DomainConfigurationSingleton.get().getInputReaderBatchSize();

	public final int numHashAreas;
	private final CSVReader reader;
	private final String[] header;

	private final List<TableEntry>[] entries;


	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReadHeaderMessage.class, this::handle)
				.onMessage(ReadBatchMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReadHeaderMessage message) {
		message.getReplyTo().tell(new DependencyMiner.HeaderMessage(this.id, this.header));
		return this;
	}

	private Behavior<Message> handle(ReadBatchMessage message) throws IOException, CsvValidationException {
		boolean replySent = false;
		while(true){
			String[] line = this.reader.readNext();
			if(line == null)
				break;

			for(int i = 0; i < line.length; i++){
				int hashAreaId = ((line[i].hashCode() % this.numHashAreas) + this.numHashAreas) % this.numHashAreas;
				this.entries[hashAreaId].add(new TableEntry(line[i], i));
				if(!replySent && this.entries[hashAreaId].size() > this.batchSize){
					message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, hashAreaId, this.entries[hashAreaId]));
					replySent = true;
					this.entries[hashAreaId] = new ArrayList<>();
				}
			}
			if(replySent)
				return this;
		}

		for(int i = 0; i < this.numHashAreas; i++){
			if(this.entries[i].size() > 0){
				message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, i, this.entries[i]));
				this.entries[i] = new ArrayList<>();
				return this;
			}
		}
		message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, -1, null));
		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.reader.close();
		return this;
	}
}
