import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
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

        //event loop
        while(true){
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            // for (SelectionKey key : selectedKeys) {
            Iterator<SelectionKey> iter = selectedKeys.iterator();

            while(iter.hasNext()){
                SelectionKey key = iter.next();

                //Accepting new cleint connection
                if(key.isAcceptable()){


                    ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    SocketChannel client = server.accept();
                    if(client != null){
                        client.configureBlocking(false); //non blocking
                        client.register(selector, SelectionKey.OP_READ);
                        System.out.println("New client connected " + client.getRemoteAddress());
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
                        continue;
                    }

                    buffer.flip(); //prepare buffer to read data from it

                    if(buffer.remaining() > 0){
                        byte[] data =  new byte[buffer.remaining()];
                        buffer.get(data);
                        String input = new String(data).trim(); //convert byte to string
                        System.out.println("Recieved from client " + client.getRemoteAddress() + " " + input);
                    }    


                    //static response RESP protocol
                    String response = "+PONG\r\n";
                    ByteBuffer responseBuffer = ByteBuffer.wrap(response.getBytes());
                    client.write(responseBuffer);


                }
            }
        }
        

    }
    
}
