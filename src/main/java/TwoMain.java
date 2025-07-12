import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TwoMain {
  public static void main(String[] args) {
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket;
    Socket clientSocket = null;
    int port = 6379;
    // int port = 8000;
    // while (true) {
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      clientSocket = serverSocket.accept();
      while (true) {
        // byte[] buffer = new byte[1024];
        clientSocket.getInputStream().read(new byte[1024]);
        // String input = new String(buffer).trim();
        // OutputStream os = clientSocket.getOutputStream();
        // os.write

        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
      }
      // serverSocket.close();
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
    // }

  }
}
