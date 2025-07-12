import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Set;

public class Main {
    public static void main(String [] args) throws IOException{
        System.out.println("Logs from your program will appear here!");
        int port = 6379;

        Selector selector = Selector.open();
        
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(port));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);  
        System.out.println("Stared redis server on port " + port);  

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        HashMap<SocketChannel, StringBuilder> clientBuffers = new HashMap<>();

        //event loop
        while(true){
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            // Iterator<SelectionKey> iter = selectedKeys.iterator();
            // while(iter.hasNext()){
                
            for (SelectionKey key : selectedKeys) {
                //     SelectionKey key = iter.next();
                
                //Accepting new cleint connection
                if(key.isAcceptable()){

                    ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    SocketChannel client = server.accept();
                    if(client != null){
                        client.configureBlocking(false); //non blocking
                        client.register(selector, SelectionKey.OP_READ);
                        System.out.println("New client connected " + client.getRemoteAddress());
                        clientBuffers.put(client, new StringBuilder()); 
                    }
                } 
                
                //Client already exists
                else if (key.isReadable()){
                    SocketChannel client = (SocketChannel) key.channel();
                    buffer.clear(); //reset buffer for new read

                    int bytesRead = client.read(buffer);

                    if(bytesRead == -1){
                        System.out.println("Client diconnected " + client.getRemoteAddress()); 
                        client.close();
                        clientBuffers.remove(client);
                        continue;
                    }

                    buffer.flip(); //prepare buffer to read data from it

                    if(buffer.remaining() > 0){
                        byte[] data =  new byte[buffer.remaining()];
                        buffer.get(data);
                        String input = new String(data).trim(); //convert byte to string
                        clientBuffers.get(client).append(input);

                        String fullInputString = clientBuffers.get(client).toString();

                        String [] lines = fullInputString.split("\r\n");
                        if(lines.length >= 3 && lines[0].startsWith("*")){
                            String command = lines[2].trim().toUpperCase();
                            
                            if(command.equals("PING")){
                                System.out.println("Response to client " + client.getRemoteAddress() + ": " + "PONG");
                                client.write(ByteBuffer.wrap("+PONG\r\n".getBytes()));
                            } else if(command.equals("ECHO") && lines.length >= 5){
                                String messageResponse = "$" + lines[4].length() + "\r\n" + lines[4] + "\r\n";
                                System.out.println("Response to client " + client.getRemoteAddress() + ": " + messageResponse);
                                client.write(ByteBuffer.wrap(messageResponse.getBytes()));
                            } else {
                                String messageErrorResponse = "-ERR unknown command '" + command + "'\r\n";
                                System.out.println("Response to client " + client.getRemoteAddress() + ": " + "Error Command: " + command);
                                client.write(ByteBuffer.wrap(messageErrorResponse.getBytes()));
                            }
                            clientBuffers.get(client).setLength(0);
                        }
                        
                        
                        
                        //static response RESP protocol

                        // String response = "+PONG\r\n";
                        // ByteBuffer responseBuffer = ByteBuffer.wrap(response.getBytes());
                        // client.write(responseBuffer);
                    }    
                }
            }
        }
    }  
}
