import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Controller {

    private final int cport;
    private final int replication;
    private final int timeout;
    private final Rebalancer rebalancer;

    private final Map<Socket, Integer> reloadTries = new ConcurrentHashMap<>();
    protected final Map<Integer, DstoreModel> dstores = new ConcurrentHashMap<>();
    protected final Map<String, Index> indices = new ConcurrentHashMap<>();

    public Controller(int cport, int replication, int timeout, int rebalance) {
        this.cport = cport;
        this.replication = replication;
        this.timeout = timeout;
        rebalancer = new Rebalancer(rebalance, this);
    }

    public static void main(String[] args) {
        try {
            int cport = Integer.parseInt(args[0]);
            int rFactor = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalancePeriod = Integer.parseInt(args[3]);

            Controller controller = new Controller(cport, rFactor, timeout, rebalancePeriod);
            new Thread(controller::launchDeadStoreThread).start();
            controller.listen();
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Command line arguments have not been provided correctly");
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Command line arguments must be integers");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace();
        }

    }

    private void launchDeadStoreThread() {
        while (true) {
            synchronized (dstores) {
                try {
                    Iterator<Map.Entry<Integer, DstoreModel>> iterator = dstores.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Integer, DstoreModel> entry = iterator.next();
                        Integer key = entry.getKey();
                        DstoreModel dstoreModel = entry.getValue();
                        if (dstoreModel.isDead()) {
                            System.out.println("Deleting the Dstore " + key + " from the list of Dstores as it is dead");
                            iterator.remove(); // Remove the DstoreModel using the iterator
                            synchronized (indices) {
                                indices.forEach((s, index) -> {
                                    if (index.getStoredByKeys().contains(dstoreModel.getPort())) {
                                        index.removeFromStoredBy(dstoreModel.getPort());
                                    }
                                });
                                System.out.println("\tDeleted the Dstore " + key + " stored dstores are now: " + dstores);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void listen() {
        try (ServerSocket server = new ServerSocket(cport)) {
            while (true) {
                Socket client = server.accept();
                new Thread(() -> {
                    try {
                        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        String message = "";
                        message = in.readLine();
                        if (message != null) {
                            if (rebalancer.getIsRebalancing().get()) {rebalancer.addToRequestQueue(new Message(message, client));return;}
                            String[] splitMessage = message.split(" ");
                            if (splitMessage[0].equals(Protocol.JOIN_TOKEN)) {
                                joinDstore(client, splitMessage);
                            } else {
                                handleMessage(client, splitMessage);
                                listenToMessage(client, in);
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("There was an error trying to establish a connection with the client");
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenToMessage(Socket client, BufferedReader in) {
        String clientMessage = "";
        try {
            while ((clientMessage = in.readLine()) != null) {
                if (rebalancer.getIsRebalancing().get()) {rebalancer.addToRequestQueue(new Message(clientMessage, client));return;}
                String[] splitMessage = clientMessage.split(" ");
                System.out.println("Client has been connected: " + client.getPort());
                handleMessage(client, splitMessage);
            }
        } catch (IOException e) {
            System.err.println("Error reading client message: " + e.getMessage());
        } finally {
            try {
                System.out.println("Closing the client " + client.getPort());
                in.close();
                client.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    public synchronized void joinDstore(Socket client, String[] splitMessage) {
        int dPort = Integer.parseInt(splitMessage[1]);
        System.out.println("Dstore has joined " + dPort);
        dstores.put(dPort, new DstoreModel(client, dPort, timeout));
        if (!(dstores.size() < replication) && !indices.isEmpty()) {
            System.out.println(indices);
            rebalancer.startReBalanceOperation();
        }
    }

    public void handleMessage(Socket client, String[] message) {
        System.out.println("Message received: " + Arrays.toString(message) + " from: " + client);
        if (dstores.size() < replication) {
            send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, client);
            System.out.println("Not enought Dstores to remove files");
            return;
        }
        switch (message[0]) {
            // Messages from client
            case Protocol.LIST_TOKEN -> list(client);
            case Protocol.STORE_TOKEN -> store(client, message[1], message[2]);
            case Protocol.LOAD_TOKEN -> load(client, message[1]);
            case Protocol.RELOAD_TOKEN -> reload(client, message[1]);
            case Protocol.REMOVE_TOKEN -> remove(client, message[1]);
            default -> System.out.println("Malformed message received " + Arrays.toString(message));
        }
    }

    private void list(Socket client) {
        System.out.println("Client: " + client.getPort() + " requesting for LIST of files");
        System.out.println("There are " + indices.size() + " files stored in the Controller");
        if (indices.isEmpty()) {
            send(Protocol.LIST_TOKEN, client);
            System.out.println("Client: " + client.getPort() + " received message: " + Protocol.LIST_TOKEN);
            return;
        }

        StringBuilder message = new StringBuilder(Protocol.LIST_TOKEN);
        synchronized (indices) {
            indices.forEach((name, dIndex) -> {
                System.out.println("LIST: File " + name + " has status " + dIndex.getStatus());
                if (dIndex.getStatus() == Index.Status.STORE_COMPLETE) {
                    System.out.println("FIle " + dIndex.getFilename() + " being put into LIST");
                    message.append(" ").append(dIndex.getFilename());
                }
            });
        }
        send(String.valueOf(message), client);
        System.out.println("Client: " + client.getPort() + " received message: " + message);
    }

    private void store(Socket client, String fileName, String fileSize) {
        System.out.println("Storing the file " + fileName);
        // Step 2: now need to notify client on where to store the files
        try {
            var file = new Index(Integer.parseInt(fileSize), fileName);
            var message = new StringBuilder(Protocol.STORE_TO_TOKEN);

            if (checkIfFileAlreadyExists(client, file)) {
                return;
            }

            // Step 3: selecting the DStores to store the file and sending the message to the client
            // Need a better function to select r DStores. This works for now
            var selectedDstores = selectDstores(new ArrayList<>());
            for (DstoreModel s : selectedDstores) {
                message.append(" ").append(s.getPort());
            }
            CountDownLatch latch = new CountDownLatch(replication);
            send(String.valueOf(message), client);
            waitForStoreACKs(file, selectedDstores, latch);

            if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                file.setStatus(Index.Status.STORE_COMPLETE);
                send(Protocol.STORE_COMPLETE_TOKEN, client);
            } else {
                synchronized (indices) {
                    indices.remove(fileName);
                }
            }
        } catch (NumberFormatException e) {
            System.err.println("There was an error when creating the DIndex file because of invalid fileSize param");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("There was an unknown error when creating the DIndex to store or when sending the STORE_TO message to client: " + client.getPort());
            e.printStackTrace();
        }
    }

    private void load(Socket client, String fileName) {
        System.out.println("Loading the file " + fileName);
        synchronized (reloadTries) {
            int tries;
            if (reloadTries.containsKey(client)) {
                tries = reloadTries.get(client);
            } else {
                tries = 0;
                reloadTries.put(client, 0);
            }
            AtomicInteger dPort = new AtomicInteger(-1);
            AtomicLong fileSize = new AtomicLong(-1);
            System.out.println("Looking for " + fileName + " in our server...");
            AtomicBoolean errorLoad = new AtomicBoolean(false);
            AtomicBoolean breakPoint = new AtomicBoolean(false);
            synchronized (indices) {
                indices.forEach((indexName, dIndex) -> {
                    if (breakPoint.get()) {
                        return;
                    }
                    if (indexName.equals(fileName) && dIndex.getStoredByKeys().size() > tries) {
                        Integer port = dIndex.getStoredByKeys().get(tries);
                        System.out.println("Found the file " + fileName + " it is stored by " + port + " and has fileSize " + dIndex.getFilesize());
                        dPort.set(port);
                        fileSize.set(dIndex.getFilesize());
                        breakPoint.set(true);
                        errorLoad.set(false);
                    } else {
                        errorLoad.set(true);
                    }
                });
            }

            if (!errorLoad.get()) {
                if (dPort.get() < 0 || fileSize.get() < 0) {
                    System.out.println("Informing the client that the file " + fileName + " was not found");
                    send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, client);
                } else {
                    System.out.println("Telling the client to get the file " + fileName + " from the DStore " + dPort);
                    send(Protocol.LOAD_FROM_TOKEN + " " + dPort + " " + fileSize, client);
                }
            } else {
                send(Protocol.ERROR_LOAD_TOKEN, client);
                synchronized (reloadTries) {
                    reloadTries.remove(client);
                }
            }
        }
    }

    private void reload(Socket client, String fileName) {
        synchronized (reloadTries) {
            System.out.println("Client had trouble doing the LOAD function, will try performing RELOAD");
            reloadTries.put(client, reloadTries.get(client) + 1);
            load(client, fileName);
        }
    }

    private synchronized void remove(Socket client, String fileName) {
        System.out.println("Removing the file " + fileName);
        Index index;
        ArrayList<Integer> storedBy;

        synchronized (indices) {
            index = indices.get(fileName);
            if (index == null || index.getStatus() != Index.Status.STORE_COMPLETE) {
                send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, client);
                return;
            }
            storedBy = index.getStoredByKeys();
        }

        CountDownLatch latch = new CountDownLatch(storedBy.size());
        waitForRemoveACKs(storedBy, fileName, index, latch);

        try {
            System.out.println("Checking that latch has finished");
            if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                synchronized (indices) {
                    index.setStatus(Index.Status.REMOVE_COMPLETE);
                    indices.remove(fileName, index);
                    send(Protocol.REMOVE_COMPLETE_TOKEN, client);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.err.println("Timed out waiting for remove ACKs");
        }
    }

    private void waitForRemoveACKs(ArrayList<Integer> storedBy, String fileName, Index index, CountDownLatch latch) {
        storedBy.forEach(integer -> {
            new Thread(() -> {
                try {
                    var toSend = Protocol.REMOVE_TOKEN + " " + fileName;
                    var expected = Protocol.REMOVE_ACK_TOKEN + " " + fileName;
                    String message = dstores.get(integer).sendAndWaitForResponse(toSend, expected);
                    System.out.println("REMOVE: Message received: " + message);
                    if (message != null) {
                        System.out.println("REMOVE LATCH COUNTING DOWN FOR " + fileName);
                        synchronized (index) {
                            index.removeFromStoredBy(integer);
                        }
                        latch.countDown();
                    } else {
                        System.out.println("Was expecting REMOVE ACK but got: null");
                    }
                } catch (DeadStoreException e) {
                    e.printStackTrace();
                }
            }).start();
        });
    }

    private void waitForStoreACKs(Index dIndex, ArrayList<DstoreModel> selectedDstores, CountDownLatch latch) {
        for (DstoreModel dstoreModel : selectedDstores) {
            new Thread(() -> {
                try {
                    String receivedMessage = dstores.get(dstoreModel.getPort()).receive(Protocol.STORE_ACK_TOKEN + " " + dIndex.getFilename());
                    if (receivedMessage != null) {
                        try {
                            dIndex.getStoredBy().put(dstoreModel.getPort(), dstoreModel.getSocket());
                            latch.countDown();
                        } catch (Exception e) {
                            //Log error
                            System.err.println("Error processing store ack from dstore " + dstoreModel.getPort());
                            e.printStackTrace();
                        }
                    } else {
                        //Log error
                        System.err.println("Dstore " + dstoreModel.getPort() + " timed out receiving STORE_ACK for " + dIndex.getFilename());
                    }
                } catch (DeadStoreException e) {
                    System.err.println("Store for " + dIndex.getFilename() + " failed due to dead dstore");
                    e.printStackTrace();
                }
            }).start();
        }
    }

    private boolean checkIfFileAlreadyExists(Socket client, Index dIndex) {
        synchronized (indices) {
            if (indices.containsKey(dIndex.getFilename())) {
                var index = indices.get(dIndex.getFilename());
                if (index.getStatus() == Index.Status.REMOVE_COMPLETE) {
                    indices.remove(dIndex.getFilename());
                } else {
                    send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN, client);
                }
                return true;
            } else {
                indices.put(dIndex.getFilename(), dIndex);
                return false;
            }
        }
    }

    private ArrayList<DstoreModel> selectDstores(ArrayList<Integer> excludedDstores) {
        var dStores = new ArrayList<DstoreModel>();
        System.out.println("Selecting Dstores...");
        AtomicInteger i = new AtomicInteger();
        AtomicInteger numOfFiles = new AtomicInteger(indices.size());
        System.out.println("Dstores: " + this.dstores);
        System.out.println("Excluded Dstores: " + excludedDstores);
        System.out.println("Indicies: " + indices);
        while (i.get() != replication) {
            dStores.sort(Comparator.comparing(DstoreModel::getNumberOfFiles));
            synchronized (dstores) {
                this.dstores.forEach(((integer, dstoreModel) -> {
                    if (!excludedDstores.contains(dstoreModel.getPort())) {
                        float x = replication;
                        float y = indices.size();
                        float z = dstores.size();
                        var floor = Math.floor((x * y) / z);
                        var ceiling = Math.ceil((x * y) / z);
                        if (i.get() == replication) {
                            return;
                        }
                        System.out.println("\tFloor: " + x + " * " + y + " / " + z + " = " + floor);
                        System.out.println("\tCeling: " + x + " * " + y + " / " + z + " = " + ceiling);
                        System.out.println("\tAmount stored: " + dstoreModel.getNumberOfFiles());
                        if (dstoreModel.getNumberOfFiles() < floor) {
                            System.out.println("\tSelected the DStore " + dstoreModel.getPort());
                            dStores.add(dstoreModel);
                            i.getAndIncrement();
                            numOfFiles.getAndIncrement();
                            return;
                        }
                        if (dstoreModel.getNumberOfFiles() >= floor && dstoreModel.getNumberOfFiles() <= ceiling) {
                            System.out.println("\tSelected the DStore " + dstoreModel.getPort());
                            dStores.add(dstoreModel);
                            i.getAndIncrement();
                            numOfFiles.getAndIncrement();
                        }
                    }
                }));
            }
        }
        return dStores;
    }

    private void send(String message, Socket socket) {
        try {
            PrintWriter socketWriter = new PrintWriter(socket.getOutputStream());
            socketWriter.print(message);
            socketWriter.println();
            socketWriter.flush();
            System.out.println(message + " sent to " + socket.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getReplication() {
        return replication;
    }

    public int getTimeout() {
        return timeout;
    }
}
