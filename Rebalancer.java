import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Rebalancer {

    private final int REBALANCE_INTERVAL;
    private final Controller controller;
    private Timer timer;
    private Integer timeout;
    private final AtomicBoolean isRebalancing = new AtomicBoolean(false);
    private final Queue<Message> requestQueue = new LinkedList<>();

    private Map<Integer, ArrayList<String>> currentFileAlloc = new HashMap<>();
    private Map<Integer, ArrayList<String>> rebalanceFileAlloc = new HashMap<>();
    private final Map<Integer, String> fileToRemoveFromDstore = new HashMap<>();
    private final Map<String, ArrayList<DstoreModel>> fileToSendToDstores = new HashMap<>();

    public Rebalancer(Integer timeout, Controller controller) {
        this.controller = controller;
        this.REBALANCE_INTERVAL = timeout;
        this.timeout = timeout;
        timer = new Timer();
        timer.schedule(new RebalanceTask(), timeout, timeout);
    }

    public void startReBalanceOperation() {
        // Stopping the timer
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        isRebalancing.set(true);
        System.out.println("\n-=-=-=-=-=  Re-balance operation starting   =-=-=-=-=-");
        reBalanceOperation();
        System.out.println("-=-=-=-=-=  Re-balance operation ended   =-=-=-=-=-\n");
        executeRequestedMessages();
        // Reseting the timer
        resetTimer();
        isRebalancing.set(false);
    }

    private void reBalanceOperation() {
        // TODO: step 1 message all the dstores LIST and wait for their response
        HashMap<Integer, ArrayList<String>> currentDstoreFileAlloc = new HashMap<>();
        controller.dstores.forEach(((integer, dstoreModel) -> {
            try {
                var dList = dstoreModel.sendAndWaitForResponse(Protocol.LIST_TOKEN, Protocol.LIST_TOKEN);
                System.out.println("REMOVE: Message received: " + dList);
                if (dList != null) {
                    if (dList.equals(Protocol.LIST_TOKEN)) {
                        currentDstoreFileAlloc.put(dstoreModel.getPort(), new ArrayList<>());
                    } else {
                        currentDstoreFileAlloc.put(dstoreModel.getPort(), new ArrayList<>(Arrays.stream(dList.split(" ")).toList().subList(1, dList.split(" ").length)));
                    }
                } else {
                    System.out.println("Was expecting REMOVE ACK but got: null");
                }
            } catch (DeadStoreException e) {
                System.out.println("Dstore " + integer + " is dead cannot retrieve files from it");
            }
        }));

        // TODO: step 2 ensure that all dstores have files allocated evenly
        if (!isSpreadCorrectly(currentDstoreFileAlloc)) {
            return;
        }

        // TODO: step 2.1 if files are evenly allocated rebalance done
        this.currentFileAlloc = currentDstoreFileAlloc;
        this.rebalanceFileAlloc = spreadFiles(removeFilesFromDeadDstores(currentDstoreFileAlloc));
        sendRebalance();
    }

    private Map<Integer, ArrayList<String>> removeFilesFromDeadDstores(Map<Integer, ArrayList<String>> oldDstoreFiles) {
        System.out.println("Checking if any files need to be deleted due to dead Dstores");
        Map<Integer, ArrayList<String>> finalDstoreFiles = new HashMap<>();
        List<String> nonDeadFiles = new ArrayList<>();
        oldDstoreFiles.forEach((integer, strings) -> {
            ArrayList<String> files = new ArrayList<>();
            strings.forEach(file -> {
                Index index = null;
                if (controller.indices.containsKey(file)) index = controller.indices.get(file);
                if (index == null) return;
                if (index.getStatus() == Index.Status.STORE_COMPLETE) {
                    files.add(file);
                    if (nonDeadFiles.contains(file)) nonDeadFiles.add(file);
                } else {
                    System.out.println("Removing the file " + file + " from indices as it was stored by a dead Dstore");
                    controller.indices.remove(file);
                }
            });
            finalDstoreFiles.put(integer, files);
        });
        return finalDstoreFiles;
    }

    private boolean isSpreadCorrectly(HashMap<Integer, ArrayList<String>> currentDstoreFileAlloc) {
        if (controller.dstores.size() == 0) return true;
        var needSpread = new AtomicBoolean(false);
        float x = controller.getReplication();
        float y = controller.indices.size();
        float z = controller.dstores.size();
        var floor = Math.floor((x * y) / z);
        var ceiling = Math.ceil((x * y) / z);
        System.out.println(currentDstoreFileAlloc.toString());
        currentDstoreFileAlloc.forEach((dPort, sFile) -> {
            if (sFile.size() < floor) {
                needSpread.set(true);
            }
            if (sFile.size() > ceiling) {
                needSpread.set(true);
            }
        });
        System.out.println("The re-balancer will need re-balancing " + needSpread.get());
        return needSpread.get();
    }

//    private void checkFilesAreEvenlyStored() {
//        System.out.println("Checking that the files are stored evenly across all Dstores");
//        if (controller.dstores.size() == 0) {
//            return;
//        }
//        var needSpread = new AtomicBoolean(false);
//        float x = controller.getReplication();
//        float y = controller.indices.size();
//        float z = controller.dstores.size();
//        var floor = Math.floor((x * y) / z);
//        var ceiling = Math.ceil((x * y) / z);
//        var lowestStoringDstores = new ArrayList<Integer>();
//        var highestStoringDstores = new ArrayList<Integer>();
//        System.out.println(currentFileAlloc.toString());
//        rebalanceFileAlloc.forEach((dPort, sFile) -> {
//            System.out.println("Checking the Dstore " + dPort);
//            if (sFile.size() < floor) {
//                lowestStoringDstores.add(dPort);
//                needSpread.set(true);
//            }
//            if (sFile.size() > ceiling) {
//                highestStoringDstores.add(dPort);
//                needSpread.set(true);
//            }
//            if (needSpread.get()) {
//                System.out.println("\tThe Dstore " + dPort + " is not evenly storing files in the rabalanceFileAlloc:"
//                        + "\t\tFloor: " + floor + " < " + sFile.size()
//                        + "\t\tCeiling: " + ceiling + " > " + sFile.size());
//                return;
//            }
//            System.out.println("\tThe Dstore " + dPort + " is evenly storing files in the rabalanceFileAlloc:");
//            sFile.forEach(s -> System.out.println("\t\t" + s));
//        });
//        if (needSpread.get()) {
//            Collections.sort(lowestStoringDstores);
//            Collections.sort(highestStoringDstores);
//            if (!lowestStoringDstores.isEmpty() && !highestStoringDstores.isEmpty()) {
//                spreadFiles(lowestStoringDstores.get(0), highestStoringDstores.get(0));
//            } else if (!lowestStoringDstores.isEmpty()) {
//                spreadFiles(lowestStoringDstores.get(0), -1);
//            } else {
//                spreadFiles(-1, highestStoringDstores.get(0));
//            }
//        } else {
//            sendRebalance();
//        }
//    }

    private void sendRebalance() {
        System.out.println("Finally sending the re-balance");
        CountDownLatch latch = new CountDownLatch(currentFileAlloc.size());

        currentFileAlloc.forEach((cDstore, cFiles) -> {
            var message = new StringBuilder(Protocol.REBALANCE_TOKEN + " ");

            AtomicReference<Map<String, ArrayList<Integer>>> toSend = new AtomicReference<>(new HashMap<>());
            rebalanceFileAlloc.forEach((rDstore, rFiles) -> {
                if (cFiles.isEmpty() && !cDstore.equals(rDstore)) {
                    return;
                } else {
                    cFiles.stream().filter(rFiles::contains).forEach(file -> {
                        System.out.println("Checking " + cDstore + " with " + rDstore + " the file is " + file);
                        if (currentFileAlloc.get(rDstore).contains(file)) {
                            return;
                        }
                        System.out.println(toSend.get().get(file));
                        if (toSend.get().get(cDstore) == null) {
                            var dstore = new ArrayList<Integer>();
                            dstore.add(rDstore);
                            toSend.get().put(file, dstore);
                        } else {
                            toSend.get().get(file).add(rDstore);
                        }

                        controller.indices.forEach((fileName, index) -> {
                            if (fileName.equals(file)) {
                                try {
                                    System.out.println("Updating the file " + file + " to sat it is stored by " + rDstore + " inside indicies:");
                                    index.getStoredBy().put(rDstore, new Socket(InetAddress.getLoopbackAddress(), rDstore));
                                    System.out.println("\t" + index.getStoredBy());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        });

                        System.out.println(toSend.get().get(file));
                    });
                }
            });
            message.append(toSend.get().size()).append(" ");
            toSend.get().forEach((file, dStores) -> {
                message.append(file).append(" ").append(dStores.size()).append(" ");
                dStores.forEach(dStore -> {
                    message.append(dStore).append(" ");
                });
            });

            controller.dstores.values().forEach(dstoreModel -> {
                rebalanceFileAlloc.forEach((ds, files) -> {
                    if (dstoreModel.getPort() == ds) {
                        System.out.println("Updated the DstoreModel " + ds + " to say it is storing " + files.size() + " files");
                        dstoreModel.setNumberOfFiles(files.size());
                    }
                });
            });


            AtomicReference<Set<String>> toRemove = new AtomicReference<>(new HashSet<>());
            currentFileAlloc.forEach((rDstore, rFiles) -> {
                if (cDstore.equals(rDstore)) {
                    cFiles.stream().filter(file -> !rFiles.contains(file)).forEach(s -> toRemove.get().add(s));
                }
            });
            message.append(toRemove.get().size()).append(" ");
            toRemove.get().forEach(s -> message.append(s).append(" "));
            var expected = Protocol.REBALANCE_COMPLETE_TOKEN;

            try {
                String response = controller.dstores.get(cDstore).sendAndWaitForResponse(message.toString(), expected);
                System.out.println("REBALANCE: Message received: " + message);
                if (response != null) {
                    System.out.println("REBALANCE LATCH COUNTING DOWN FOR " + cDstore);
                    latch.countDown();
                } else {
                    System.out.println("Was expecting REBALANCE_COMPLETE_TOKEN but got: null");
                }
            } catch (DeadStoreException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            if (latch.await(controller.getTimeout(), TimeUnit.MILLISECONDS)) {
                System.out.println("Re-balance successfully completed");
            } else {
                System.out.println("Timed out while waiting for the Dstore responses when performing re-balance");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Map<Integer, ArrayList<String>> spreadFiles(Map<Integer, ArrayList<String>> dstoreFiles) {
        //        System.out.println("Performing a spread of files");
//        if (lowestStoringDstore != -1 && highestStoringDstore != -1) {
//            var lowFiles = rebalanceFileAlloc.get(lowestStoringDstore);
//            var highFiles = currentFileAlloc.get(highestStoringDstore).stream().filter(string -> !lowFiles.contains(string)).toList();
//            System.out.println("Adding file " + highFiles.get(0) + " to " + lowestStoringDstore);
//            System.out.println("Deleting file " + highFiles.get(0) + " from " + highestStoringDstore);
//            addToRebalanceFileAlloc(lowestStoringDstore, highFiles.get(0));
//            deleteFromRebalanceFileAlloc(highestStoringDstore, highFiles.get(0));
//        } else if (lowestStoringDstore != -1) {
//            var lowFiles = rebalanceFileAlloc.get(lowestStoringDstore);
//            AtomicReference<Integer> highFilesDstore = new AtomicReference<>(controller.dstores.values().stream().filter(dstoreModel -> dstoreModel.getPort() != lowestStoringDstore).findFirst().get().getPort());
//            rebalanceFileAlloc.forEach((integer, files) -> {
//                if (rebalanceFileAlloc.get(highFilesDstore.get()).size() < files.size()) {
//                    highFilesDstore.set(integer);
//                }
//            });
//            System.out.println(highFilesDstore.get());
//            var highFiles = rebalanceFileAlloc.get(highFilesDstore.get()).stream().filter(string -> !lowFiles.contains(string)).toList();
//            System.out.println("Adding file " + highFiles.get(0) + " to " + lowestStoringDstore);
//            System.out.println("Deleting file " + highFiles.get(0) + " from " + highFilesDstore.get());
//            addToRebalanceFileAlloc(lowestStoringDstore, highFiles.get(0));
//            deleteFromRebalanceFileAlloc(highFilesDstore.get(), highFiles.get(0));
//        } else {
//            AtomicReference<Integer> lowFilesDstore = new AtomicReference<>((Integer) rebalanceFileAlloc.keySet().toArray()[0]);
//            AtomicReference<Integer> highFilesDstore = new AtomicReference<>((Integer) rebalanceFileAlloc.keySet().toArray()[0]);
//            rebalanceFileAlloc.forEach((integer, files) -> {
//                if (rebalanceFileAlloc.get(highFilesDstore.get()).size() < files.size()) {
//                    highFilesDstore.set(integer);
//                }
//                if (rebalanceFileAlloc.get(lowFilesDstore.get()).size() > files.size()) {
//                    lowFilesDstore.set(integer);
//                }
//            });
//            var lowFiles = rebalanceFileAlloc.get(lowFilesDstore.get());
//            var highFiles = rebalanceFileAlloc.get(highFilesDstore.get()).stream().filter(string -> !lowFiles.contains(string)).toList();
//            System.out.println("Adding file " + highFiles.get(0) + " to " + lowFilesDstore);
//            System.out.println("Deleting file " + highFiles.get(0) + " from " + highFilesDstore);
//            addToRebalanceFileAlloc(lowFilesDstore.get(), highFiles.get(0));
//            deleteFromRebalanceFileAlloc(highFilesDstore.get(), highFiles.get(0));
//        }
//        checkFilesAreEvenlyStored();
        Map<String, Integer> fileStoredByAmount = new HashMap<>();
        dstoreFiles.keySet().forEach(ds -> dstoreFiles.get(ds).forEach(file -> fileStoredByAmount.merge(file, 1, Integer::sum)));

        fileStoredByAmount.keySet().forEach(file -> {
            System.out.println("Checking " + file + " is properly balanced across all Dstores");
            if (fileStoredByAmount.get(file) == controller.getReplication()) {
                System.out.println("The file " + file + " is properly balanced");
                return;
            }
            if (fileStoredByAmount.get(file) > controller.getReplication()) {
                System.out.println("File " + file + " is over the replication amount: " + fileStoredByAmount.get(file) + " > " + controller.getReplication());
                dstoreFiles.keySet().forEach(ds -> {
                    if (dstoreFiles.get(ds).contains(file)) {
                        System.out.println("Found a Dstore that contains the file " + file + ", proceeding to REMOVE it...");
                        dstoreFiles.get(ds).remove(file);
                        fileStoredByAmount.put(file, fileStoredByAmount.get(file) - 1);
                        System.out.println("\t" + file + " was removed from " + ds);
                    }
                });
            }
            if (fileStoredByAmount.get(file) < controller.getReplication()) {
                System.out.println("File " + file + " is under the replication amount: " + fileStoredByAmount.get(file) + " > " + controller.getReplication());
                dstoreFiles.keySet().forEach(ds -> {
                    if (dstoreFiles.get(ds).contains(file)) {
                        System.out.println("Found a Dstore that contains the file " + file + ", proceeding to ADD TO it...");
                        dstoreFiles.get(ds).add(file);
                        fileStoredByAmount.put(file, fileStoredByAmount.get(file) + 1);
                        System.out.println("\t" + file + " added to " + ds);
                    }
                });
            }
        });

        return finalReBalanceOfFiles(fileStoredByAmount, dstoreFiles);
    }

    private Map<Integer, ArrayList<String>> finalReBalanceOfFiles(Map<String, Integer> fileStoredByAmount, Map<Integer, ArrayList<String>> dstoreFiles) {
        float x = controller.getReplication();
        float y = fileStoredByAmount.size();
        float z = dstoreFiles.size();
        var floor = Math.floor((x * y) / z);
        var ceiling = Math.ceil((x * y) / z);
        Integer lowestStoringDstore = getLowestStoringDstore(dstoreFiles);
        Integer highestStoringDstore = getHighestStoringDstore(dstoreFiles);

        // Using a timeout in case that the re-balancing goes on for an infinite amount of time
        var currentTime = System.currentTimeMillis();
        var stoppingTime = currentTime + controller.getTimeout();
        while ((dstoreFiles.get(highestStoringDstore).size() > ceiling || dstoreFiles.get(lowestStoringDstore).size() < floor) && (System.currentTimeMillis() < stoppingTime)) {
            for (String thisFile : dstoreFiles.get(highestStoringDstore)) {
                //        if (needSpread.get()) {
//            Collections.sort(lowestStoringDstores);
//            Collections.sort(highestStoringDstores);
//            if (!lowestStoringDstores.isEmpty() && !highestStoringDstores.isEmpty()) {
//                spreadFiles(lowestStoringDstores.get(0), highestStoringDstores.get(0));
//            } else if (!lowestStoringDstores.isEmpty()) {
//                spreadFiles(lowestStoringDstores.get(0), -1);
//            } else {
//                spreadFiles(-1, highestStoringDstores.get(0));
//            }
//        } else {
//            sendRebalance();
//        }
                if (!dstoreFiles.get(lowestStoringDstore).contains(thisFile)) {
                    dstoreFiles.get(lowestStoringDstore).add(thisFile);
                    dstoreFiles.get(highestStoringDstore).remove(thisFile);
                    break;
                }
            }
            lowestStoringDstore = getLowestStoringDstore(dstoreFiles);
            highestStoringDstore = getHighestStoringDstore(dstoreFiles);
        }
        return dstoreFiles;
    }

    private Integer getHighestStoringDstore(Map<Integer, ArrayList<String>> dstoreFiles) {
        final AtomicInteger[] lowestDstore = {new AtomicInteger((Integer) dstoreFiles.keySet().toArray()[0])};
        dstoreFiles.forEach((ds, files) -> {
            if (files.size() > dstoreFiles.get(lowestDstore[0].get()).size()) {
                lowestDstore[0].set(ds);
            }
        });
        return lowestDstore[0].get();
    }

    private Integer getLowestStoringDstore(Map<Integer, ArrayList<String>> dstoreFiles) {
        final AtomicInteger[] highestDstore = {new AtomicInteger((Integer) dstoreFiles.keySet().toArray()[0])};
        dstoreFiles.forEach((ds, files) -> {
            if (files.size() < dstoreFiles.get(highestDstore[0].get()).size()) {
                highestDstore[0].set(ds);
            }
        });
        return highestDstore[0].get();
    }


    private void executeRequestedMessages() {
        requestQueue.forEach(message -> {
            var splitMessage = message.getMessage().split(" ");
            if (splitMessage[0].equals(Protocol.JOIN_TOKEN)) {
                controller.joinDstore(message.getReuqester(), splitMessage);
            } else {
                controller.handleMessage(message.getReuqester(), splitMessage);
            }
        });
    }

    public synchronized void resetTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        timer = new Timer();
        timer.schedule(new RebalanceTask(), timeout, REBALANCE_INTERVAL);
    }

    private class RebalanceTask extends TimerTask {
        @Override
        public void run() {
            startReBalanceOperation();
        }
    }

    public AtomicBoolean getIsRebalancing() {
        return isRebalancing;
    }

    public void addToRequestQueue(Message message) {
        System.out.println("Adding the message " + message.getMessage() + " to request queue in the re-balancer");
        requestQueue.add(message);
    }

    public void addToRebalanceFileAlloc(Integer key, String files) {
        var newFiles = new ArrayList<>(rebalanceFileAlloc.get(key));
        newFiles.add(files);
        rebalanceFileAlloc.put(key, newFiles);
    }

    public void deleteFromRebalanceFileAlloc(Integer key, String files) {
        System.out.println(rebalanceFileAlloc);
        System.out.println(key);
        System.out.println(rebalanceFileAlloc.get(key));
        var newFiles = new ArrayList<String>(rebalanceFileAlloc.get(key));
        newFiles.remove(files);
        rebalanceFileAlloc.put(key, newFiles);
    }
}
