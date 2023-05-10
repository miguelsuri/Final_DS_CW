import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Index {

    private String filename;
    private long filesize;
    private Map<Integer, Socket> storedBy = new HashMap<>();
    private Status status;
    private int storeACKs;
    private int removeACKs;

    public static enum Status {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    public Index(long filesize, String filename) {
        this.status = Status.STORE_IN_PROGRESS;
        this.filesize = filesize;
        this.filename = filename;
        this.storeACKs = 0;
    }

    public long getFilesize() {
        return filesize;
    }

    public void setFilesize(long filesize) {
        this.filesize = filesize;
    }

    public Map<Integer, Socket> getStoredBy() {
        return storedBy;
    }

    public void setStoredBy(Map<Integer, Socket> storedBy) {
        this.storedBy = storedBy;
    }

    public ArrayList<Integer> getStoredByKeys() {
        var x = new ArrayList<Integer>();
        storedBy.forEach((integer, socket) -> {
            x.add(integer);
        });
        return x;
    }

    public ArrayList<Socket> getStoredByValues() {
        var x = new ArrayList<Socket>();
        storedBy.forEach((integer, socket) -> {
            x.add(socket);
        });
        return x;
    }

    public void removeFromStoredBy(Integer dstore) {
        storedBy.remove(dstore);
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public int getStoreACKs() {
        return storeACKs;
    }

    public void setStoreACKs(int storeACKs) {
        this.storeACKs = storeACKs;
    }

    public int getRemoveACKs() {
        return removeACKs;
    }

    public void setRemoveACKs(int removeACKs) {
        this.removeACKs = removeACKs;
    }

}
