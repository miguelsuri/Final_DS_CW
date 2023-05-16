import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Dstore {

    private final int port; // The port the Dstore listens to
    private final int cport; // The controllers port
    private int timeout; // Timeout in millisecondsF
    private final File fileFolder; // Where to store the data locally
    private Long amountStored;
    private Socket cSocket;
    protected BufferedReader controllerIn;
    protected PrintWriter controllerOut;


    public Dstore(int port, int cport, int timeout, String fileFolderName) throws Exception {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;

        this.fileFolder = new File(fileFolderName);
        if (fileFolder.exists() && !fileFolder.isDirectory()) {
            throw new Exception("Folder name provided exists as a file and not a directory");
        } else if (!fileFolder.exists()) {
            System.out.println("New folder being created");
            if (!fileFolder.mkdir()) throw new Exception("Folder could not be created");
        }
    }

    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(args[0]);
            int cport = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            String fileForlder = args[3];

            var dStore = new Dstore(port, cport, timeout, fileForlder);
            dStore.listen();
        } catch (NumberFormatException e) {
            System.err.println("Error with arguments when starting the DStore");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Error occurred when creating the Controllers socket inside the DStore");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error occurred when creating the DStore due to the folder name having an existing file name");
            e.printStackTrace();
        }

    }

    public void listen() {
        joinDstore();
        new Thread(this::launchControllerThread).start();
        launchClientMessageHandler();
    }

    private void launchClientMessageHandler() {
        try (ServerSocket server = new ServerSocket(port)) {
            while (true) {
                Socket client = server.accept();
                new Thread(() -> {
                    try {
                        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        String message = in.readLine();
                        if (message != null) {
                            System.out.println("Message received: " + message + " from: " + client);
                            handleMessage(client, message.split(" "));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void launchControllerThread() {
        while (true) {
            try {
                String message = controllerIn.readLine();
                if (message != null) {
                    System.out.println("Message received: " + message + " from: " + cSocket);
                    handleMessage(cSocket, message.split(" "));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleMessage(Socket client, String[] message) {
        switch (message[0]) {
            // Messages from client
            case Protocol.STORE_TOKEN -> store(client, message[1], message[2], false);
            case Protocol.REMOVE_TOKEN -> remove(client, message[1]);
            case Protocol.LOAD_DATA_TOKEN -> load(client, message[1]);

            // Message from controller
            case Protocol.LIST_TOKEN -> list(client);
            case Protocol.REBALANCE_TOKEN -> rebalance(message);
            case Protocol.REBALANCE_STORE_TOKEN -> store(client, message[1], message[2], true);

            default -> System.err.println("Malformed message received " + Arrays.toString(message));
        }
    }

    private void store(Socket client, String fileName, String fileSize, boolean isRebalance) {
        System.out.println("Storing the file " + fileName + " in DStore " + port);
        // Send ACK to the client that we have gotten the message
        System.out.println("Sending ACK to client to get file contents");
        send(Protocol.ACK_TOKEN, client);

        try {
            System.out.println("Storing the file " + fileName + " in the folder " + fileFolder);
            OutputStream writer = new FileOutputStream(new File(fileFolder, fileName), false);
            InputStream reader = client.getInputStream();

            //Receive + write file content from client
            byte[] nextLine = new byte[1000];
            int len;
            do {
                len = reader.readNBytes(nextLine, 0, 1000);
                writer.write(nextLine, 0, len);
                writer.flush();
            }
            while (len == 1000);
            System.out.println("File finished storing closing the InputStream of the client " + client.getPort());
            writer.close();
            System.out.println("File " + fileName + " is stored in folder " + fileFolder);

            // Send a message to the Controller to notify the file has been stored
            if (isRebalance) {
                return;
            }
            System.out.println("Sending ACK to controller");
            send(Protocol.ACK_TOKEN, client);
//        send(Protocol.STORE_ACK_TOKEN + " " + fileName, cSocket);
            synchronized (controllerOut) {
                controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
            }
        } catch (IOException e) {
            System.err.println("There was an error when reading the file contents from the client " + client.getPort());
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void remove(Socket client, String fileName) {
        System.out.println("Remove of " + fileName + " has been requested by Controller");
        try {
            Path filePath = new File(fileFolder, fileName).toPath();
            System.out.println("File " + filePath + " was found, attempting to remove it");
            if (filePath.toFile().delete()) {
                System.out.println("Deleted the file: " + filePath);
                synchronized (controllerOut) {
                    controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                }
            } else {
                System.out.println("Failed to delete the file " + fileName);
                synchronized (controllerOut) {
                    controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                }
            }
        } catch (Exception e) {
            System.err.println("An error when trying to delete the file " + fileName + " in Dstore " + port);
            e.printStackTrace();
        }
    }

    private void load(Socket client, String fileName) {
        System.out.println("DStore " + port + " is loading the file " + fileName);
        try {
            try (FileInputStream reader = new FileInputStream(new File(fileFolder, fileName))) {
                reader.transferTo(client.getOutputStream());
            } catch (FileNotFoundException e) {
                System.out.println("There was no file " + fileName + " inside the dstore");
                e.printStackTrace();
            }
            System.out.println("The file " + fileName + " has been transferred to the client " + client.getPort());
        } catch (FileNotFoundException e) {
            System.err.println("There was an error creating the FileInputStream because the " + fileName + " cannot be found in folder " + fileFolder);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("There was an error getting the OutputStream of the client " + client.getPort());
            e.printStackTrace();
        }
    }

    private void list(Socket client) {
        System.out.println("Controller is asking for LIST");
        var message = new StringBuilder(Protocol.LIST_TOKEN + " ");
        if (Objects.requireNonNull(fileFolder.listFiles()).length == 0) {
            controllerOut.println(message);
        } else {
            Arrays.stream(Objects.requireNonNull(fileFolder.listFiles())).forEach(file -> {
                message.append(file.getName()).append(" ");
            });
            synchronized (controllerOut) {
                controllerOut.println(message);
            }
        }
    }

    private void rebalance(String[] message) {
        System.out.println("Rebalance message received");

        try {
            Map<Integer, ArrayList<String>> toSend = new HashMap<>();
            ArrayList<String> toRemove = new ArrayList<>();
            int numberToSend = Integer.parseInt(message[1]);
            int totalReceivers = 0;
            int index = 2;

            for (int i = 0; i < numberToSend; i++) {
                String name = message[index];
                index++;

                int numberOfReceivers = Integer.parseInt(message[index]);
                totalReceivers += numberOfReceivers;
                index++;
                for (int j = 0; j < numberOfReceivers; j++) {
                    Integer receiver = Integer.parseInt(message[index]);
                    if (!toSend.containsKey(receiver)) {
                        toSend.put(receiver, new ArrayList<String>());
                    }
                    toSend.get(receiver).add(name);
                    index++;
                }
            }

            int numberToRemove = Integer.parseInt(message[index]);
            index++;
            for (int k = 0; k < numberToRemove; k++) {
                toRemove.add(message[index]);
                index++;
            }

            rebalanceRemove(toRemove);

            CountDownLatch latch = new CountDownLatch(totalReceivers);
            waitForRebalanceStoreACKs(toSend, latch);

            if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
                System.out.println("Re-balance store successfully completed");
                controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
            } else {
                System.out.println("Timed out while waiting for the Dstore responses when performing re-balance");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void waitForRebalanceStoreACKs(Map<Integer, ArrayList<String>> toSend, CountDownLatch latch) {
        for (Integer dstore : toSend.keySet()) {
            for (String filename : toSend.get(dstore)) {
                new Thread(() -> {
                    try {
                        System.out.println("Sending re-balance file " + filename + " to dstore " + dstore);
                        Socket socket = new Socket(InetAddress.getLocalHost(), dstore);
                        File file = new File(fileFolder.getPath() + File.separator + filename);
                        String message = Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + file.length();
                        send(message, socket);

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String receivedMessage = in.readLine();
                        if (!receivedMessage.equals(Protocol.ACK_TOKEN)) {
                            System.err.println("Dstore " + dstore + " should have sent ACK but " + port + " received " + receivedMessage);
                            return;
                        }
                        rebalanceSendFileContents(socket, filename);
                        in.close();
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }
        }
    }

    private void rebalanceSendFileContents(Socket socket, String filename) throws IOException {
        byte[] content = new byte[256];
        int len;
        FileInputStream fileIn = new FileInputStream(new File(fileFolder, filename));
        OutputStream fileOut = socket.getOutputStream();
        do {
            len = fileIn.read(content);
            if (len >= 0) {
                fileOut.write(content, 0, len);
                fileOut.flush();
            }
        } while (len > 0);
        fileIn.close();
        fileOut.close();
    }

    private void rebalanceRemove(ArrayList<String> toRemove) {
        for (String filename : toRemove) {
            System.out.println("Removing file " + filename);
            new File(fileFolder, filename).delete();
        }
    }


//        new Thread(() -> {
//            //Interpret files to send and files to remove from the message
//            Map<Integer, List<String>> filesToSend;
//            String[] filesToRemove;
//            int index;
//
//            String tmessage = "";
//            for(String s : message) {
//                tmessage = tmessage + " " + s;
//            }
//            System.out.println("Interpreting message:" + tmessage);
//            int numberToSend = Integer.parseInt(message[1]);
//            int totalReceivers = 0;
//            index = 2;
//            filesToSend = new HashMap<Integer,List<String>>();
//            for(int i=0; i<numberToSend; i++) {
//                String name = message[index];
//                index++;
//
//                int numberOfReceivers = Integer.parseInt(message[index]);
//                totalReceivers += numberOfReceivers;
//                index++;
//                for(int j=0; j<numberOfReceivers; j++) {
//                    Integer receiver = Integer.parseInt(message[index]);
//                    if(!filesToSend.containsKey(receiver)) {
//                        filesToSend.put(receiver,new ArrayList<String>());
//                    }
//                    filesToSend.get(receiver).add(name);
//                    index++;
//                }
//            }
//
//            int numberToRemove = Integer.parseInt(message[index]);
//            index++;
//            filesToRemove = new String[numberToRemove];
//            for(int k=0; k<numberToRemove; k++) {
//                filesToRemove[k] = message[index];
//                index++;
//            }
//            System.out.println("Interpreting complete, will send " + numberToSend + " and remove " + numberToRemove);
//
//            //Send each file to send to the Dstore at the specified port number
//            CountDownLatch latch = new CountDownLatch(totalReceivers);
//            for(Integer dstore : filesToSend.keySet()) {
//                for(String filename : filesToSend.get(dstore)) {
//                    new Thread(() -> {
//                        try {
//                            System.out.println("Sending " + filename + " to store " + dstore);
//                            Socket socket = new Socket(InetAddress.getLocalHost(), dstore.intValue());
//                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//                            File f = new File(fileFolder.getPath() + File.separator + filename);
//                            long fileSize = f.length();
//                            String dstoreMessage = Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + fileSize;
//                            out.println(dstoreMessage);
//
//                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                            String receivedMessage = in.readLine();
//                            if(!receivedMessage.equals(Protocol.ACK_TOKEN)) {
//                                //Log error
//                                System.err.println("Dstore " + dstore + " should have sent ACK but " + port + " received " + receivedMessage);
//                            }
//
//                            byte[] content = new byte[256];
//                            int len;
//                            FileInputStream fileIn = new FileInputStream(new File(fileFolder, filename));
//                            OutputStream fileOut = socket.getOutputStream();
//                            do {
//                                len = fileIn.read(content);
//                                if(len >= 0) {
//                                    fileOut.write(content, 0, len);
//                                    fileOut.flush();
//                                }
//                            }
//                            while(len > 0);
//                            fileIn.close();
//                            fileOut.close();
//                            in.close();
//                            out.close();
//                            socket.close();
//                        }
//                        catch(IOException e) {
//                            e.printStackTrace();
//                        }
//                        finally {
//                            try {latch.countDown();} catch(Exception e) {}
//                        }
//                    }).start();
//                }
//            }
//            try {latch.await(timeout, TimeUnit.MILLISECONDS);} catch(Exception e) {e.printStackTrace();}
//
//            //Remove each file to remove from fileFolder
//            for(String filename : filesToRemove) {
//                System.out.println("Removing file " + filename);
//                new File(fileFolder, filename).delete();
//            }
//
//            //Send REBALANCE_COMPLETE message to client (the controller)
//            try {
//                send(Protocol.REBALANCE_COMPLETE_TOKEN, new Socket(InetAddress.getLoopbackAddress(), cport));
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }).start();

    private void joinDstore() {
        try {
            Socket cSocket = new Socket(InetAddress.getLocalHost(), cport);
            this.cSocket = cSocket;
            controllerIn = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
            controllerOut = new PrintWriter(cSocket.getOutputStream(), true);
            String joinMessage = Protocol.JOIN_TOKEN + " " + port;
            send(Protocol.JOIN_TOKEN + " " + port, cSocket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
