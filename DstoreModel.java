import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class DstoreModel {

    private Socket socket;
    private int port;
    private BufferedReader reader;
    private PrintWriter writer;
    private int timeout;
    private boolean dead;
    private final ArrayList<String> messageQueue;
    private int numberOfFiles;

    public DstoreModel(Socket socket, int port, int timeout) {
        this.socket = socket;
        this.port = port;
        this.timeout = timeout;
        dead = false;
        try {
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new PrintWriter(socket.getOutputStream(), true);
            messageQueue = new ArrayList<>();
            new Thread(this::start).start();
        } catch (IOException e) {
            dead = true;
            throw new RuntimeException(e);
        }
    }

    public void start() {
        while (!dead) {
            String message = null;
            try {
                message = reader.readLine();
                System.out.println("Message received: " + message + " from: " + socket);
                if(message == null) {
                    System.out.println("DSTORE DIED");
                    dead = true;
                } else {
                    synchronized (messageQueue) {
                        messageQueue.add(message);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String receive(String expectedMessages) throws DeadStoreException {
        if (!dead) {
            String receivedMessage = null;
            var start = System.currentTimeMillis();
            var end = start + timeout;
            while(receivedMessage == null && System.currentTimeMillis() < end) {
                receivedMessage = getMessageFromQueue(expectedMessages);
                if (dead) {throw new DeadStoreException(String.valueOf(this.getPort()));}
            }
            return receivedMessage;
        } else {
            throw new DeadStoreException(String.valueOf(this.getPort()));
        }
    }

    public String sendAndWaitForResponse(String message, String expectedMessages) throws DeadStoreException {
        if (dead) throw new DeadStoreException("Tried to send and receive but DStore is dead");
        synchronized(writer) {
            writer.println(message);
            writer.flush();
        }
        return receive(expectedMessages);
    }


//    public String sendAndWaitForResponse(String toSend, String expected) throws DeadStore {
//        if (dead) throw new DeadStore("Tried to send and receive but DStore is dead");
//        synchronized (writer) {
//            writer.println(toSend);
//        }
//        return receive(expected);
//    }

    private String getMessageFromQueue(String expectedMessages) {
        AtomicReference<String> returnVal = new AtomicReference<>(null);
        synchronized (messageQueue) {
            messageQueue.stream().filter(s -> s.equals(expectedMessages)).findFirst().ifPresent(returnVal::set);
            messageQueue.remove(returnVal.get());
        }
        return returnVal.get();
    }

    public Socket getSocket() {
        return socket;
    }

    public int getPort() {
        return port;
    }

    public BufferedReader getReader() {
        return reader;
    }

    public PrintWriter getWriter() {
        return writer;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isDead() {
        return dead;
    }

    public ArrayList<String> getMessageQueue() {
        return messageQueue;
    }

    public int getNumberOfFiles() {
        return numberOfFiles;
    }
}
