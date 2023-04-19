import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Controller {

    private int cport;
    private int replication;
    private int timeout;
    private int rebalance;

    private final Map<Socket, Integer> reloadTries;
    protected final Map<Integer, DstoreModel> dstores;
    protected final Map<String, Index> indices;

    public Controller(int cport, int replication, int timeout, int rebalance) {
        this.cport = cport;
        this.replication = replication;
        this.timeout = timeout;
        this.rebalance = rebalance;
        dstores = Collections.synchronizedMap(new HashMap<>());
        indices = Collections.synchronizedMap(new HashMap<>());
        reloadTries = Collections.synchronizedMap(new HashMap<>());
    }

    public static void main(String[] args) {
        try {
            int cport = Integer.parseInt(args[0]);
            int rFactor = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalancePeriod = Integer.parseInt(args[3]);

            Controller controller = new Controller(cport, rFactor, timeout, rebalancePeriod);
            controller.listen();
        } catch (IndexOutOfBoundsException e) {
            System.err.println("Command line arguments have not been provided correctly");
            return;
        } catch (NumberFormatException e) {
            System.err.println("Command line arguments must be integers");
            return;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

    }

    private void listen() {
        try {
            ServerSocket server = new ServerSocket(cport);
            while (true) {
                Socket client = server.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String message = "";
                message = in.readLine();
                if (message != null) {
                    String[] splitMessage = message.split(" ");

                    new Thread(() -> {
                        if (splitMessage[0].equals(Protocol.JOIN_TOKEN)) {
                            int dPort = Integer.parseInt(splitMessage[1]);
                            System.out.println("Dstore has joined " + dPort);
                            dstores.put(dPort, new DstoreModel(client, dPort, timeout));
                        } else {
                            System.out.println("Client has connected");
                            messageReceived(client, Arrays.toString(splitMessage));
                            handleMessage(client, splitMessage);
                            String clientMessage = "";
                            do {
                                try {
                                    clientMessage = in.readLine();
                                    messageReceived(client, clientMessage);
                                    if (clientMessage != null) {
                                        var splitClientMessage = clientMessage.split(" ");
                                        handleMessage(client, splitClientMessage);
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            } while (clientMessage != null);
                            try {
                                System.out.println("Closing the client " + client.getPort());
                                in.close();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }).start();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleMessage(Socket client, String[] message) {
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
//            case Protocol.REMOVE_TOKEN -> remove(client, message[1]);
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
                synchronized (dIndex.getStatus()) {
                    System.out.println("LIST: File " + name + " has status " + dIndex.getStatus() + " and has " + dIndex.getStoreACKs() + " ACKs");
                    if (dIndex.getStatus() == Index.Status.STORE_COMPLETE) {
                        System.out.println("FIle " + dIndex.getFilename() + " being put into LIST");
                        message.append(" ").append(dIndex.getFilename());
                    }
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
        }
    }

    private void load(Socket client, String fileName) {
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
                reloadTries.remove(client);
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


    private void waitForStoreACKs(Index dIndex, ArrayList<DstoreModel> selectedDstores, CountDownLatch latch) {
        for (DstoreModel dstoreModel : selectedDstores) {
            new Thread(() -> {
                try {
                    String receivedMessage = dstores.get(dstoreModel.getPort()).receive(Protocol.STORE_ACK_TOKEN + " " + dIndex.getFilename());
                    if (receivedMessage != null) {
                        try {
                            System.out.println("Increasing store ACK count of " + dIndex.getFilename() + " from " + dIndex.getStoreACKs() + " to " + (dIndex.getStoreACKs() + 1));
                            dIndex.getStoredBy().put(dstoreModel.getPort(), dstoreModel.getSocket());
                            dIndex.setStoreACKs(dIndex.getStoreACKs() + 1);
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
                } catch (DeadStore e) {
                    System.err.println("Store for " + dIndex.getFilename() + " failed due to dead dstore");
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


    private void messageReceived(Socket client, String message) {
        System.out.println("Message received: " + message + " from " + client);
    }

    private void send(String message, Socket socket) {
        try {
            PrintWriter socketWriter = new PrintWriter(socket.getOutputStream());
            socketWriter.print(message);
            socketWriter.println();
            socketWriter.flush();
            System.out.println(message + " sent to " + socket.getPort());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
