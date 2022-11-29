package de.ddm.structures;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class BatchContainer {

    private final int columnNumber;
    private final List<List<String[]>>batches;

    public BatchContainer(int columnNumber){
        this.columnNumber = columnNumber;
        this.batches = new ArrayList<List<String[]>>();
    }

    public void addBatch(List<String[]> batch){
        this.batches.add(batch);
    }

    public String[] getColumnBatch(int columnId, int batchId){
        return batches.get(batchId).get(columnId);
    }

    public int getColumnNumber(){
        return this.columnNumber;
    }

    public int getBatchNumber(){
        return batches.size();
    }
}
