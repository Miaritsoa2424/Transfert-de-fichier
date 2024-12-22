import java.io.*;
import java.net.*;
import java.util.*;

public class ClientApplication {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private Scanner scanner;

    public static void main(String[] args) {
        ClientApplication app = new ClientApplication();
        app.run();
    }

    public ClientApplication() {
        scanner = new Scanner(System.in);
    }

    private void run() {
        while (true) {
            System.out.println("Entrez l'adresse IP du serveur :");
            String ip = scanner.nextLine();
            System.out.println("Entrez le port du serveur :");
            int port = Integer.parseInt(scanner.nextLine());

            connectToServer(ip, port);

            if (socket != null && !socket.isClosed()) {
                while (true) {
                    System.out.println("\nQue voulez-vous faire ?");
                    System.out.println("1. Lister les fichiers");
                    System.out.println("2. Télécharger un fichier");
                    System.out.println("3. Envoyer un fichier");
                    System.out.println("4. Supprimer un fichier");
                    System.out.println("5. Quitter");

                    String choice = scanner.nextLine();

                    switch (choice) {
                        case "1":
                            listFiles();
                            break;
                        case "2":
                            downloadFile();
                            break;
                        case "3":
                            uploadFile();
                            break;
                        case "4":
                            deleteFile();
                            break;
                        case "5":
                            disconnect();
                            return;
                        default:
                            System.out.println("Choix invalide.");
                            break;
                    }
                }
            }
        }
    }

    private void connectToServer(String ip, int port) {
        try {
            socket = new Socket(ip, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Connecté au serveur.");
        } catch (IOException e) {
            System.out.println("Échec de la connexion : " + e.getMessage());
        }
    }

    private void listFiles() {
        if (socket == null || socket.isClosed()) {
            System.out.println("Non connecté à un serveur.");
            return;
        }

        try {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF("list");
            dos.flush();

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String response = dis.readUTF();

            if (response.startsWith("Fichiers disponibles")) {
                System.out.println("Fichiers disponibles :");
                String[] files = response.split(":")[1].trim().split(", ");
                for (String file : files) {
                    System.out.println(file);
                }
            } else {
                System.out.println(response);
            }
        } catch (IOException ex) {
            System.out.println("Erreur lors de l'affichage des fichiers : " + ex.getMessage());
        }
    }

    private void uploadFile() {
        if (socket == null || socket.isClosed()) {
            System.out.println("Non connecté à un serveur.");
            return;
        }

        System.out.println("Entrez le chemin du fichier à envoyer :");
        String filePath = scanner.nextLine();
        File file = new File(filePath);

        if (!file.exists()) {
            System.out.println("Fichier non trouvé.");
            return;
        }

        try {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF("upload");
            dos.writeUTF(file.getName());
            dos.writeLong(file.length());

            try (FileInputStream fileIn = new FileInputStream(file)) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = fileIn.read(buffer)) != -1) {
                    dos.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("Fichier envoyé avec succès !");
        } catch (IOException ex) {
            System.out.println("Erreur lors de l'envoi du fichier : " + ex.getMessage());
        }
    }

    private void downloadFile() {
        if (socket == null || socket.isClosed()) {
            System.out.println("Non connecté à un serveur.");
            return;
        }

        System.out.println("Entrez le nom du fichier à télécharger :");
        String fileName = scanner.nextLine();

        try {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF("download");
            dos.writeUTF(fileName);

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String response = dis.readUTF();

            if (response.startsWith("Downloading")) {
                String fileNameToSave = response.split(":")[1];
                long fileLength = dis.readLong();

                System.out.println("Enregistrement du fichier sous : " + fileNameToSave);
                try (FileOutputStream fileOut = new FileOutputStream(fileNameToSave)) {
                    byte[] buffer = new byte[4096];
                    long totalRead = 0;
                    int bytesRead;

                    while (totalRead < fileLength &&
                            (bytesRead = dis.read(buffer, 0, (int) Math.min(buffer.length, fileLength - totalRead))) != -1) {
                        fileOut.write(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
                }

                System.out.println("Fichier téléchargé avec succès !");
            } else {
                System.out.println(response);
            }
        } catch (IOException ex) {
            System.out.println("Erreur lors du téléchargement du fichier : " + ex.getMessage());
        }
    }

    private void deleteFile() {
        if (socket == null || socket.isClosed()) {
            System.out.println("Non connecté à un serveur.");
            return;
        }

        System.out.println("Entrez le nom du fichier à supprimer :");
        String fileName = scanner.nextLine();

        try {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF("DELETE_FILE");
            dos.writeUTF(fileName);

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String response = dis.readUTF();
            if ("SUCCESS".equals(response)) {
                System.out.println("Fichier supprimé avec succès.");
            } else {
                System.out.println("Erreur : " + dis.readUTF());
            }
        } catch (IOException ex) {
            System.out.println("Erreur lors de la suppression du fichier : " + ex.getMessage());
        }
    }

    private void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                System.out.println("Déconnecté du serveur.");
            }
        } catch (IOException e) {
            System.out.println("Erreur lors de la déconnexion : " + e.getMessage());
        }
    }
}
