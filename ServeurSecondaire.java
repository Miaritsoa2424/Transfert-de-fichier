import java.io.*;
import java.net.*;

public class ServeurSecondaire {
    private static int port = 5002;  // Le port sur lequel le serveur secondaire écoute pour recevoir des fichiers

    public static void main(String[] args) {
        System.out.println("Démarrage du serveur secondaire...");

        // Lancement du serveur dans un thread séparé
        new Thread(() -> startServer()).start();
    }

    private static void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Serveur secondaire démarré sur le port " + port + ".");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new FileReceiveHandler(clientSocket).start();
            }
        } catch (IOException e) {
            System.out.println("Erreur du serveur secondaire : " + e.getMessage());
        }
    }

    static class FileReceiveHandler extends Thread {
        private Socket clientSocket;

        public FileReceiveHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try (
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
            ) {
                String command = dis.readUTF();

                if ("store".equals(command)) {
                    receiveFile(dis, dos);
                } else if ("GET_PART".equals(command)) {
                    handleGetPart(dis, dos);
                } else if ("DELETE_PART".equals(command)) {
                    handleDeletePart(dis, dos);
                } else {
                    dos.writeUTF("Commande non reconnue");
                }

            } catch (IOException e) {
                System.out.println("Erreur avec le client : " + e.getMessage());
            }
        }

        private void receiveFile(DataInputStream dis, DataOutputStream dos) {
            try {
                // Lire le nom et la taille du fichier
                String fileName = dis.readUTF();
                long fileSize = dis.readLong();

                // Créer le fichier local pour stocker la partie du fichier
                File file = new File("storage/" + fileName);
                file.getParentFile().mkdirs();  // Créer les répertoires nécessaires si inexistants

                try (FileOutputStream fos = new FileOutputStream(file)) {
                    byte[] buffer = new byte[4096];
                    long totalRead = 0;
                    int bytesRead;

                    // Lire et écrire les données du fichier
                    while (totalRead < fileSize && (bytesRead = dis.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
                }

                System.out.println("Fichier " + fileName + " reçu et sauvegardé.");
                dos.writeUTF("Fichier reçu et sauvegardé avec succès.");
            } catch (IOException e) {
                try {
                    dos.writeUTF("Erreur lors de la réception du fichier : " + e.getMessage());
                } catch (IOException ioException) {
                    System.out.println("Erreur lors de l'envoi du message d'erreur au client : " + ioException.getMessage());
                }
                System.out.println("Erreur lors de la réception du fichier : " + e.getMessage());
            }
        }

        private void handleGetPart(DataInputStream dis, DataOutputStream dos) throws IOException {
            String partName = dis.readUTF();
            File partFile = new File("storage", partName); // Répertoire contenant les parties

            if (partFile.exists() && partFile.isFile()) {
                dos.writeUTF("PART_FOUND");
                dos.writeLong(partFile.length()); // Envoie la taille du fichier

                try (FileInputStream fis = new FileInputStream(partFile)) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        dos.write(buffer, 0, bytesRead);
                    }
                }
            } else {
                dos.writeUTF("PART_NOT_FOUND");
            }
        }

        private void handleDeletePart(DataInputStream dis, DataOutputStream dos) throws IOException {
            String partName = dis.readUTF();
            File partFile = new File("storage/" + partName); // Chemin des fichiers dans le serveur secondaire
        
            if (partFile.exists() && partFile.delete()) {
                dos.writeUTF("SUCCESS");
            } else {
                dos.writeUTF("FAILURE");
            }
        }
    }
}
