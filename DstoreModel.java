import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

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
                messageReceived(socket, message);

                if(message == null) {
                    //Disconnect
                    dead = true;
//                    remover.removeDstore(port, socket);
                }
                else {
                    messageQueue.add(message);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String receive(String expectedMessages) throws DeadStore {
        if (!dead) {
            String receivedMessage = null;
            var start = System.currentTimeMillis();
            var end = start + timeout;
            while(receivedMessage == null && System.currentTimeMillis() < end) {
                receivedMessage = getMessageFromQueue(expectedMessages);
                if (dead) {throw new DeadStore(String.valueOf(this.getPort()));}
            }
            return receivedMessage;
        } else {
            throw new DeadStore(String.valueOf(this.getPort()));
        }
    }

    private String getMessageFromQueue(String expectedMessages) {
        synchronized (messageQueue) {
            for (String message : getMessageQueue()) {
                if (message.equals(expectedMessages)) {
                    return message;
                }
            }
        }
        return null;
    }


    private void messageReceived(Socket client, String message) {
        System.out.println("DSTORE_MODEL: Message received: " + message + " from " + client);
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
