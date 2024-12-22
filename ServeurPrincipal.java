import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ServeurPrincipal {

    private static final List<StorageServerInfo> storageServers = new ArrayList<>();

    public static void main(String[] args) {
        loadStorageServerConfig("config.txt");

        // Démarrer le serveur principal dans le terminal
        startServer();
    }

    private static void loadStorageServerConfig(String configFilePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(configFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(":");
                if (parts.length == 2) {
                    String ip = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    storageServers.add(new StorageServerInfo(ip, port));
                }
            }
            System.out.println("Configuration des serveurs de stockage chargée.");
        } catch (IOException e) {
            System.out.println("Erreur lors du chargement de la configuration : " + e.getMessage());
        }
    }

    private static void startServer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(5000)) {
                System.out.println("Serveur principal démarré sur le port 5000.");
                System.out.println("Serveur principal en attente de connexions...");

                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new ClientHandler(clientSocket).start();
                }
            } catch (IOException e) {
                System.out.println("Erreur du serveur principal : " + e.getMessage());
            }
        }).start();
    }

    static class ClientHandler extends Thread {
        private final Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try (
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
            ) {
                while (true) {
                    String operation = dis.readUTF();
                    switch (operation) {
                        case "list":
                            listFiles(dos);
                            break;
        
                        case "upload":
                            receiveAndDistributeFile(dis, dos);
                            break;
        
                        case "download":
                            String fileName = dis.readUTF();
                            downloadFile(fileName, dos);
                            break;
        
                        case "DELETE_FILE":
                            String fileToDelete = dis.readUTF();
                            handleDeleteFile(fileToDelete, dos);
                            break;
        
                        default:
                            dos.writeUTF("Commande non reconnue");
                            break;
                    }
                }
            } catch (IOException e) {
                System.out.println("Connexion client terminée : " + e.getMessage());
            }
        }

        private void receiveAndDistributeFile(DataInputStream dis, DataOutputStream dos) throws IOException {
            String fileName = dis.readUTF();
            long fileSize = dis.readLong();

            File tempFile = new File("temp_" + fileName);
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[4096];
                long totalRead = 0;
                int read;

                while (totalRead < fileSize && (read = dis.read(buffer)) != -1) {
                    fos.write(buffer, 0, read);
                    totalRead += read;
                }
            }

            System.out.println("Fichier reçu : " + fileName);

            List<File> parts = splitFile(tempFile, 2);
            for (int i = 0; i < parts.size(); i++) {
                if (i < storageServers.size()) {
                    sendFileToStorageServer(parts.get(i), storageServers.get(i), fileName);
                } else {
                    System.out.println("Pas assez de serveurs pour distribuer toutes les parties.");
                }
            }

            tempFile.delete();
            for (File part : parts) {
                part.delete();
            }

            dos.writeUTF("Fichier distribué avec succès.");
        }

        private void listFiles(DataOutputStream dos) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader("file_mapping.txt"))) {
                Set<String> files = new HashSet<>();
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 1) {
                        files.add(parts[0]);
                    }
                }
                dos.writeUTF("Fichiers disponibles : " + String.join(", ", files));
            } catch (IOException e) {
                dos.writeUTF("Erreur lors de la lecture des fichiers disponibles : " + e.getMessage());
            }
        }

        private List<File> splitFile(File file, int partCount) throws IOException {
            List<File> parts = new ArrayList<>();
            try (FileInputStream fis = new FileInputStream(file)) {
                long partSize = file.length() / partCount;
                byte[] buffer = new byte[4096];

                for (int i = 0; i < partCount; i++) {
                    File part = new File(file.getName() + ".part" + (i + 1));
                    try (FileOutputStream fos = new FileOutputStream(part)) {
                        long written = 0;
                        int read;
                        while (written < partSize && (read = fis.read(buffer)) != -1) {
                            fos.write(buffer, 0, read);
                            written += read;
                        }
                    }
                    parts.add(part);
                }
            }
            return parts;
        }

        private void sendFileToStorageServer(File part, StorageServerInfo serverInfo, String fileName) {
            try (Socket socket = new Socket(serverInfo.ip, serverInfo.port);
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 FileInputStream fis = new FileInputStream(part)) {

                dos.writeUTF("store");
                dos.writeUTF(part.getName());
                dos.writeLong(part.length());

                byte[] buffer = new byte[4096];
                int read;
                while ((read = fis.read(buffer)) != -1) {
                    dos.write(buffer, 0, read);
                }

                System.out.println("Partie " + part.getName() + " envoyée à " + serverInfo);
                updateFileMapping(fileName, part.getName(), serverInfo);
            } catch (IOException e) {
                System.out.println("Erreur lors de l'envoi de " + part.getName() + " à " + serverInfo + " : " + e.getMessage());
            }
        }

        private void updateFileMapping(String fileName, String partName, StorageServerInfo serverInfo) {
            try (FileWriter fw = new FileWriter("file_mapping.txt", true);
                 BufferedWriter bw = new BufferedWriter(fw)) {
                bw.write(fileName + "," + partName + "," + serverInfo.ip + ":" + serverInfo.port);
                bw.newLine();
                System.out.println("Fichier de suivi mis à jour pour " + partName);
            } catch (IOException e) {
                System.out.println("Erreur lors de la mise à jour du fichier de suivi : " + e.getMessage());
            }
        }

        private void downloadFile(String fileName, DataOutputStream dos) {
    try {
        File assembledFile = assembleFileFromParts(fileName);
        
        // Créer le dossier "Download" s'il n'existe pas
        File downloadDir = new File("Download");
        if (!downloadDir.exists()) {
            downloadDir.mkdir();
        }

        // Déplacer le fichier assemblé dans le dossier "Download"
        File finalFile = new File(downloadDir, fileName);
        assembledFile.renameTo(finalFile);

        dos.writeUTF("Downloading:" + finalFile.getName());
        dos.writeLong(finalFile.length());

        try (FileInputStream fis = new FileInputStream(finalFile)) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = fis.read(buffer)) != -1) {
                dos.write(buffer, 0, read);
            }
        }
    } catch (IOException e) {
        try {
            dos.writeUTF("Erreur lors du téléchargement : " + e.getMessage());
        } catch (IOException ignored) {
        }
    }
}

        private File assembleFileFromParts(String requestedFile) throws IOException {
            List<String> mappingLines = Files.readAllLines(Paths.get("file_mapping.txt"));
            List<String> partsToAssemble = new ArrayList<>();
            Map<String, String> partToServerMap = new HashMap<>();

            for (String line : mappingLines) {
                String[] tokens = line.split(",");
                if (tokens[0].equals(requestedFile)) {
                    partsToAssemble.add(tokens[1]);
                    partToServerMap.put(tokens[1], tokens[2]);
                }
            }

            if (partsToAssemble.isEmpty()) {
                throw new IOException("Aucune partie trouvée pour " + requestedFile);
            }

            File tempDir = new File("temp_parts");
            if (!tempDir.exists()) tempDir.mkdir();

            for (String part : partsToAssemble) {
                String serverInfo = partToServerMap.get(part);
                String[] serverDetails = serverInfo.split(":");
                String serverAddress = serverDetails[0];
                int serverPort = Integer.parseInt(serverDetails[1]);

                downloadPart(serverAddress, serverPort, part, tempDir);
            }

            File assembledFile = new File("assembled_files", requestedFile);
            if (!assembledFile.getParentFile().exists()) {
                assembledFile.getParentFile().mkdirs();
            }

            try (FileOutputStream fos = new FileOutputStream(assembledFile)) {
                for (String part : partsToAssemble) {
                    File partFile = new File(tempDir, part);
                    Files.copy(partFile.toPath(), fos);
                }
            }

            return assembledFile;
        }

        private void downloadPart(String serverAddress, int serverPort, String partName, File tempDir) throws IOException {
            if (!tempDir.exists()) {
                tempDir.mkdirs();
            }

            try (Socket socket = new Socket(serverAddress, serverPort);
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dis = new DataInputStream(socket.getInputStream());
                 FileOutputStream fos = new FileOutputStream(new File(tempDir, partName))) {

                dos.writeUTF("GET_PART");
                dos.writeUTF(partName);

                String response = dis.readUTF();
                System.out.println(response);
                if ("PART_FOUND".equals(response)) {
                    long fileSize = dis.readLong();

                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    long totalRead = 0;

                    while (totalRead < fileSize && (bytesRead = dis.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
                } else {
                    System.err.println("Erreur: Partie " + partName + " non trouvée sur " + serverAddress + ":" + serverPort);
                }
            }
        }

        private void handleDeleteFile(String fileName, DataOutputStream dos) {
            File mappingFile = new File("file_mapping.txt");
            File tempFile = new File("file_mapping_temp.txt");

            boolean fileFound = false;

            try (
                BufferedReader reader = new BufferedReader(new FileReader(mappingFile));
                BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))
            ) {
                String line;

                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 3 && parts[0].trim().equals(fileName.trim())) {
                        fileFound = true;
                        String partFileName = parts[1].trim();
                        String serverAddress = parts[2].trim();
                        deletePartFromSecondaryServer(partFileName, serverAddress);
                    } else {
                        writer.write(line);
                        writer.newLine();
                    }
                }
            } catch (IOException e) {
                try {
                    dos.writeUTF("Erreur lors de la suppression : " + e.getMessage());
                } catch (IOException ioException) {
                    System.out.println("Erreur lors de l'envoi du message d'erreur au client : " + ioException.getMessage());
                }
                System.out.println("Erreur lors du traitement du mapping : " + e.getMessage());
                return;
            }

            if (fileFound) {
                mappingFile.delete();
                tempFile.renameTo(mappingFile);
                try {
                    dos.writeUTF("Fichier " + fileName + " supprimé.");
                } catch (IOException e) {
                    System.out.println("Erreur lors de l'envoi du message de succès au client : " + e.getMessage());
                }
            } else {
                try {
                    dos.writeUTF("Fichier non trouvé : " + fileName);
                } catch (IOException e) {
                    System.out.println("Erreur lors de l'envoi du message d'erreur au client : " + e.getMessage());
                }
            }
        }

        private void deletePartFromSecondaryServer(String partFileName, String serverAddress) {
            try (Socket socket = new Socket(serverAddress.split(":")[0], Integer.parseInt(serverAddress.split(":")[1]));
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                dos.writeUTF("DELETE_PART");
                dos.writeUTF(partFileName);
                System.out.println("Partie " + partFileName + " supprimée de " + serverAddress);
            } catch (IOException e) {
                System.out.println("Erreur lors de la suppression de la partie sur " + serverAddress + ": " + e.getMessage());
            }
        }
    }

    private static class StorageServerInfo {
        String ip;
        int port;

        public StorageServerInfo(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }
    }

    
}
