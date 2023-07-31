package disi.unitn.michele.andrea;

public class DataEntry {

    private String value;
    private int version;

    public DataEntry(String value) {
        this.value = value;
        this.version = 1;
    }

    public String GetValue() {
        return this.value;
    }

    public int GetVersion() {
        return this.version;
    }

    public void SetValue(String value) {
        this.value = value;
        this.version++;
    }

    public boolean IsOutdated(DataEntry data) {
        if(data.version < this.version) {
            return true;
        } else {
            return false;
        }
    }
}



