import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

public class Dstore {

    private int port; // The port the Dstore listens to
    private int cport; // The controllers port
    private int timeout; // Timeout in millisecondsF
    private File fileFolder; // Where to store the data locally
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
        launchControllerThread();
        launchClientMessageHandler();
    }

    private void launchClientMessageHandler() {
        try {
            ServerSocket server = new ServerSocket(port);
            while(true) {
                try {
                    Socket client = server.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String message = in.readLine();
                    messageReceived(client, message);
                    handleMessage(client, message.split(" "));
                }
                catch(Exception e) {
                    //Log error
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void launchControllerThread() {
        new Thread(() -> {
            try {
                String message = controllerIn.readLine();
                if(message != null) {
                    messageReceived(cSocket, message);
                    handleMessage(cSocket, message.split(" "));
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }).start();
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
            if (isRebalance) {return;}
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
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void remove(Socket client, String fileName) {
        System.out.println("Remove of " + fileName + " has been requested by Controller");
        try {
            System.out.println("Store " + port + " removing " + fileName + "...");
            //Remove the file from fileFolder
            Path path = new File(fileFolder, fileName).toPath();

            String controllerMessage;
            if(Files.deleteIfExists(path)) {
                System.out.println("Store " + port + " removed " + fileName);
                //Send REMOVE_ACK message to client (the controller)
                synchronized(controllerOut) {
                    controllerMessage = Protocol.REMOVE_ACK_TOKEN + " " + fileName;
                }
            }
            else {
                System.out.println("Store " + port + " couldn't remove " + fileName);
                //Send DOES NOT EXIST error
                synchronized(controllerOut) {
                    controllerMessage = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName;
                }
            }
            controllerOut.println(controllerMessage);
        }
        catch(IOException e) {
            e.printStackTrace();
        }

//        System.out.println("Remove of " + fileName + " has been requested by Controller");
//        try {
//            Path filePath = new File(fileFolder, fileName).toPath();
//            System.out.println("File " + filePath + " was found, attempting to remove it");
//            if (filePath.toFile().delete()) {
//                System.out.println("Deleted the file: " + filePath);
//                synchronized (controllerOut) {
//                    controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
//                }
//            } else {
//                System.out.println("Failed to delete the file " + fileName);
//                synchronized(controllerOut) {
//                    controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
//                }
//            }
//        } catch (Exception e) {
//            System.err.println("An error when trying to delete the file " + fileName + " in Dstore " + port);
//            e.printStackTrace();
//        }
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
            try {
                send(message.toString(), new Socket(InetAddress.getLoopbackAddress(), cport));
            } catch (IOException e) {
                System.err.println("There was an error sending the LIST EMPTY message to the controller");
                e.printStackTrace();
            }
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

    }

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
