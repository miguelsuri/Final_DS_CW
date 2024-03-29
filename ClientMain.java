import java.io.File;
import java.io.IOException;

/**
 * This is just an example of how to use the provided client library. You are expected to customise this class and/or
 * develop other client classes to test your Controller and Dstores thoroughly. You are not expected to include client
 * code in your submission.
 */
public class ClientMain {

    public static void main(String[] args) throws Exception {

        final int cport = Integer.parseInt(args[0]);
        int timeout = Integer.parseInt(args[1]);

        // this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
        File downloadFolder = new File("downloads");
        if (!downloadFolder.exists())
            if (!downloadFolder.mkdir())
                throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");

        // this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
        File uploadFolder = new File("to_store");
        if (!uploadFolder.exists())
            throw new RuntimeException("to_store folder does not exist");

//        Dstore dstore1 = new Dstore(54321, 12345, 10000, "dstore_1");
//        Dstore dstore2 = new Dstore(54322, 12345, 10000, "dstore_2");
//        Dstore dstore3 = new Dstore(54323, 12345, 10000, "dstore_3");
//        Dstore dstore4 = new Dstore(54324, 12345, 10000, "dstore_4");
//
//        new Thread(dstore1::listen).start();
//        new Thread(dstore2::listen).start();
//        new Thread(dstore3::listen).start();
//        new Thread(dstore4::listen).start();
//
//        testClient(cport, timeout, downloadFolder, uploadFolder);

        // launch a number of concurrent clients, each doing the same operations
//        for (int i = 0; i < 10; i++) {
//            new Thread() {
//                public void run() {
//                    test2Client(cport, timeout, downloadFolder, uploadFolder);
//                }
//            }.start();
//        }

        testClient1(cport, timeout, downloadFolder, uploadFolder);
    }

    private static void testClient1(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {

            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

            try {
                client.connect();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            try {
                list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // store first file in the to_store folder twice, then store second file in the to_store folder once
            File fileList[] = uploadFolder.listFiles();
            if (fileList.length > 0) {
                try {
                    client.store(fileList[0]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    client.store(fileList[0]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fileList.length > 1) {
                try {
                    client.store(fileList[1]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

    public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {
            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
            client.connect();

            File fileList[] = uploadFolder.listFiles();
            for (int i = 0; i < fileList.length; i++) {
                File fileToStore = fileList[fileList.length - 1];
                try {
                    client.store(fileToStore);
                } catch (Exception e) {
                    System.out.println("Error storing file " + fileToStore);
                    e.printStackTrace();
                }
            }

            System.out.println("-=-=-=-=-=-=-=-=-=-");

            for (int i = 0; i < fileList.length; i++) {
                String fileToRemove = fileList[fileList.length - 1].getName();
                try {
                    client.remove(fileToRemove);
                } catch (Exception e) {
                    System.out.println("Error remove file " + fileToRemove);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

    public static void testRemove(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {

            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

            try {
                client.connect();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            try {
                list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // store first file in the to_store folder twice, then store second file in the to_store folder once
            File fileList[] = uploadFolder.listFiles();
            if (fileList.length > 0) {
                try {
                    client.store(fileList[0]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    client.store(fileList[1]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
//            if (fileList.length > 1) {
//                try {
//                    client.store(fileList[1]);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
            String list[] = null;
            try {
                list = list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

//            if (list != null)
//                for (String filename : list)
//                    try {
//                        client.load(filename, downloadFolder);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }

            if (list != null)
                for (String filename : list)
                    try {
                        client.remove(filename);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            if (list != null && list.length > 0)
                try {
                    client.remove(list[0]);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            try {
                list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

    public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {

            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

            try {
                client.connect();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            try {
                list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // store first file in the to_store folder twice, then store second file in the to_store folder once
            File fileList[] = uploadFolder.listFiles();
            if (fileList.length > 0) {
                try {
                    client.store(fileList[0]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    client.store(fileList[0]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fileList.length > 1) {
                try {
                    client.store(fileList[1]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            String list[] = null;
            try {
                list = list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (list != null)
                for (String filename : list)
                    try {
                        client.load(filename, downloadFolder);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

            if (list != null)
                for (String filename : list)
                    try {
                        client.remove(filename);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            if (list != null && list.length > 0)
                try {
                    client.remove(list[0]);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            try {
                list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

    public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
        System.out.println("Retrieving list of files...");
        String list[] = client.list();

        System.out.println("Ok, " + list.length + " files:");
        int i = 0;
        for (String filename : list)
            System.out.println("[" + i++ + "] " + filename);

        return list;
    }

}