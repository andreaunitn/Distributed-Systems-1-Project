package disi.unitn.michele.andrea;

public class DataEntry {

    private String value;
    private int version;

    public DataEntry(String value) {
        this.value = value;
        this.version = 1;
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



