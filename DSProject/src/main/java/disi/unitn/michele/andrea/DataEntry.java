package disi.unitn.michele.andrea;

import java.io.Serializable;

public class DataEntry implements Serializable {

    private String value;
    private int version;

    /***** Constructor *****/
    public DataEntry(String value) {
        this.value = value;
        this.version = 1;
    }

    /***** Methods *****/
    // To be used for the "empty" data entry, with version -1
    public DataEntry(String value, int version){
        this.value = value;
        this.version = version;
    }

    // Get value of the data
    public String GetValue() {
        return this.value;
    }

    // Get current version of the data
    public int GetVersion() {
        return this.version;
    }

    // Set value and version
    public void SetValue(String value, boolean update) {
        this.value = value;

        if(update) {
            this.version++;
        }
    }

    // Check if the input data is outdated
    public boolean IsOutdated(DataEntry data) {
        return data.version < this.version;
    }
}



