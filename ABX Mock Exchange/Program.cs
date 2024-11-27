using System.Net.Sockets;
using System.Text;
using Newtonsoft.Json;

class Program
{
    const int ServerPort = 3000;
    const string ServerAddress = "127.0.0.1";
    static List<Packet> packets = new List<Packet>();
    static int LastSequence = 0;

    static void Main(string[] args)
    {
        ConnectToServer();
        WritePacketsToJsonFile();
    }

    static void ConnectToServer()
    {
        TcpClient client = null;

        try
        {
            client = new TcpClient(ServerAddress, ServerPort);
            Console.WriteLine("Connected to the server.");

            using (NetworkStream stream = client.GetStream())
            {
                SendInitialRequest(stream);

                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ParsePacket(buffer, bytesRead);
                }
            }

            HandleMissingSequences();
        }
        catch (SocketException ex)
        {
            HandleSocketException(ex);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
        finally
        {
            client?.Close();
            packets.Sort((packet1, packet2) => packet1.PacketSequence - packet2.PacketSequence);
            Console.WriteLine("Connection closed.");
        }
    }

    static void SendInitialRequest(NetworkStream stream)
    {
        byte[] request = { 1, 0 }; // CallType=1, Sequence=0
        Console.WriteLine("Request sent: CallType=1, Sequence=0");
        stream.Write(request, 0, request.Length);
    }

    static void HandleMissingSequences()
    {
        int i = 1, j = 0;
        while (j < packets.Count && i < LastSequence)
        {
            while (packets[j].PacketSequence != i)
            {
                RequestMissingSequence(i++);
            }
            i++;
            j++;
        }
    }

    static void HandleSocketException(SocketException ex)
    {
        Console.WriteLine($"Socket error: {ex.Message}");

        if (ex.Message.Contains("not allowed on non-connected sockets"))
        {
            Console.WriteLine("Attempting to reconnect...");
            ReconnectAndResend();
        }
    }

    static void ParsePacket(byte[] buffer, int bytesRead)
    {
        for (int i = 0; i < bytesRead; i += 17) // Adjust step size to packet size
        {
            if (i + 17 > bytesRead) break;

            string symbol = Encoding.ASCII.GetString(buffer, i, 4).Trim('\0');
            char buySellIndicator = (char)buffer[i + 4];
            int quantity = Ntohl32(BitConverter.ToUInt32(buffer, i + 5));
            int price = Ntohl32(BitConverter.ToUInt32(buffer, i + 9));
            int packetSequence = Ntohl32(BitConverter.ToUInt32(buffer, i + 13));
            LastSequence = Math.Max(LastSequence, packetSequence);

            Console.WriteLine($"Received packet: [{packetSequence}] {symbol} {buySellIndicator} Q:{quantity} P:{price}");

            packets.Add(new Packet
            {
                PacketSequence = packetSequence,
                Symbol = symbol,
                BuySellIndicator = buySellIndicator,
                Quantity = quantity,
                Price = price
            });
        }
    }

    static int Ntohl32(uint bigEndianValue)
    {
        return BitConverter.ToInt32(BitConverter.GetBytes(bigEndianValue).Reverse().ToArray(), 0);
    }

    static void RequestMissingSequence(int sequence)
    {
        try
        {
            using (TcpClient tcpClient = new TcpClient(ServerAddress, ServerPort))
            {
                Console.WriteLine("Connected to the server.");
                using (NetworkStream stream = tcpClient.GetStream())
                {
                    byte[] request = { 2, (byte)sequence }; // CallType=2, Sequence=<sequence>
                    Console.WriteLine($"Request sent: CallType=2, Sequence={sequence}");
                    stream.Write(request, 0, request.Length);

                    byte[] buffer = new byte[17];
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        ParsePacket(buffer, bytesRead);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error while requesting missing sequence: {ex.Message}");
        }
    }

    static void ReconnectAndResend()
    {
        Console.WriteLine("Reconnecting...");
        ConnectToServer();
    }

    static void WritePacketsToJsonFile()
    {
        try
        {
            string json = JsonConvert.SerializeObject(packets, Formatting.Indented);
            File.WriteAllText("stock_packet.json", json);
            Console.WriteLine("Packets written to stock_packet.json.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error writing to file: {ex.Message}");
        }
    }

    class Packet
    {
        public string Symbol { get; set; }
        public char BuySellIndicator { get; set; }
        public int Quantity { get; set; }
        public int Price { get; set; }
        public int PacketSequence { get; set; }
    }
}
