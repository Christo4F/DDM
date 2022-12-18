package de.ddm.structures;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
public class TableEntry implements AkkaSerializable {
    private final String value;
    private final int column;
}
