using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using CoCoL;

namespace SlimDHT
{
    /// <summary>
    /// Wraps all the control logic in a simple process
    /// </summary>
    public static class ConsoleProcess
    {
        private static string HELPTEXT =
@"
SlimDHT control panel.

Overall actions:
  - help
    Displays this text
  - quit
  - exit
    Stops SlimDHT and terminates the program

Node actions:
  - check
    Checks if all nodes are running
  - node connect <ip> <port>
    Starts a node and connects to a peer
  - node start
    Starts a new node
  - node list
    Lists running nodes
  - node stop <number>
    Stops the node with the given number
  - node stat <number>
    Requests stats from the node with the given number
  - node refresh <number>
    Starts the refresh process for the node with the given number

DHT actions:
  - add <value>
    Adds the string value to the store
  - get <hash>
    Gets the item with the given hash 
  - hash <value>
    Computes the hash for the string value


";
        /// <summary>
        /// Runs the console interface
        /// </summary>
        /// <returns>An awaitable task.</returns>
        public static Task RunAsync()
        {
            // Set up a console forwarder process
            var consoleOut = Skeletons.CollectAsync(
                Channels.ConsoleOutput.ForRead, 
                x => Console.Out.WriteLineAsync(x ?? string.Empty)
            );

            // Set up a channel for sending control messages
            var inputChannel = Channel.Create<string>(buffersize: 10);

            // Set up the console reader process
            var consoleInput = AutomationExtensions.RunTask(
                new { Control = inputChannel.AsWrite() },
                async self =>
                {
                    string line;

                    // TODO: The blocking read prevents clean shutdown,
                    // but direct access to the input stream has issues with the buffer
                    while ((line = await Task.Run(() => Console.ReadLine())) != null)
                        await self.Control.WriteAsync(line);
                }
            );

            // Set up the control logic handler
            var proc = AutomationExtensions.RunTask(new
                {
                    Control = inputChannel.AsRead(),
                    Output = Channels.ConsoleOutput.ForWrite
                },
                async self =>
                {
                    var peers = new List<Tuple<PeerInfo, Task, IWriteChannel<PeerRequest>>>();
                    var rnd = new Random();

                    var portnr = 15000;

                    await self.Output.WriteAsync(HELPTEXT);
                    while (true)
                    {
                        try
                        {
                            var commandline = await self.Control.ReadAsync() ?? string.Empty;
                            var command = commandline.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? string.Empty;

                            if (string.Equals(command, "help", StringComparison.OrdinalIgnoreCase))
                                await self.Output.WriteAsync(HELPTEXT);
                            else if (string.Equals(command, "exit", StringComparison.OrdinalIgnoreCase) || string.Equals(command, "quit", StringComparison.OrdinalIgnoreCase))
                                return;
                            else if (string.Equals(command, "check", StringComparison.OrdinalIgnoreCase))
                            {
                                for (var i = peers.Count - 1; i >= 0; i--)
                                    if (await peers[i].Item3.IsRetiredAsync)
                                    {
                                        await self.Output.WriteAsync($"Peer {peers[i].Item1.Key} at {peers[i].Item1.Address} terminated");
                                        peers.RemoveAt(i);
                                    }

                                await self.Output.WriteAsync($"Completed check, found {peers.Count} live peers");
                            }
                            else if (string.Equals(command, "node", StringComparison.OrdinalIgnoreCase))
                            {
                                var actions = commandline.Split(new char[] { ' ' }, 4, StringSplitOptions.RemoveEmptyEntries);

                                if (string.Equals(actions[1], "start", StringComparison.OrdinalIgnoreCase))
                                {
                                    var pi = new PeerInfo(Key.CreateRandomKey(), new IPEndPoint(IPAddress.Loopback, portnr));

                                    await self.Output.WriteAsync($"Starting node {pi.Key} on {pi.Address}");
                                    var chan = Channel.Create<PeerRequest>();
                                    var s = Task.Run(() =>
                                                     Peer.RunPeer(
                                                         pi, 5, 100, TimeSpan.FromDays(1),
                                                         peers.Count == 0 ? new EndPoint[0] : new[] { peers[rnd.Next(0, peers.Count - 1)].Item1.Address },
                                                         chan.AsRead()
                                                     )
                                                     .ContinueWith(_ => inputChannel.WriteAsync("check"))
                                                    );

                                    peers.Add(new Tuple<PeerInfo, Task, IWriteChannel<PeerRequest>>(pi, s, chan));
                                    portnr++;
                                }
                                else if (string.Equals(actions[1], "list", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (actions.Length != 2)
                                    {
                                        await self.Output.WriteAsync("The list command takes no arguments");
                                        continue;
                                    }

                                    for (var i = 0; i < peers.Count; i++)
                                        await self.Output.WriteAsync(string.Format("{0}: {1} - {2}", i, peers[i].Item1.Key, peers[i].Item1.Address));
                                    await self.Output.WriteAsync(string.Empty);
                                }
                                else if (string.Equals(actions[1], "connect", StringComparison.OrdinalIgnoreCase))
                                {
                                    actions = commandline.Split(new char[] { ' ' }, 5, StringSplitOptions.RemoveEmptyEntries);
                                    if (actions.Length != 4)
                                    {
                                        await self.Output.WriteAsync("The connect command needs exactly two arguments, the ip and the port");
                                        continue;
                                    }

                                    if (!IPAddress.TryParse(actions[2], out var ip))
                                    {
                                        await self.Output.WriteAsync($"Failed to parse ip: {actions[2]}");
                                        continue;
                                    }

                                    if (!int.TryParse(actions[3], out var port))
                                    {
                                        await self.Output.WriteAsync($"Failed to parse {actions[3]} as an integer");
                                        continue;
                                    }

                                    var pi = new PeerInfo(Key.CreateRandomKey(), new IPEndPoint(IPAddress.Loopback, portnr));
                                    await self.Output.WriteAsync($"Starting node {pi.Key} on {pi.Address}");
                                    var chan = Channel.Create<PeerRequest>();

                                    var s = Task.Run(() =>
                                                          Peer.RunPeer(
                                                              pi, 5, 100, TimeSpan.FromDays(1),
                                                              new[] { new IPEndPoint(ip, port) },
                                                              chan.AsRead()
                                                          )
                                                          .ContinueWith(_ => inputChannel.WriteAsync("check"))
                                                         );

                                    peers.Add(new Tuple<PeerInfo, Task, IWriteChannel<PeerRequest>>(pi, s, chan));
                                    portnr++;

                                }
                                else if (string.Equals(actions[1], "stop", StringComparison.OrdinalIgnoreCase) || string.Equals(actions[1], "stat", StringComparison.OrdinalIgnoreCase) || string.Equals(actions[1], "refresh", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (actions.Length != 3)
                                    {
                                        await self.Output.WriteAsync($"The {actions[1]} command takes exactly one argument, the node number");
                                        continue;
                                    }

                                    if (!int.TryParse(actions[2], out var ix))
                                    {
                                        await self.Output.WriteAsync($"Failed to parse {actions[2]} as an integer");
                                        continue;
                                    }

                                    if (ix < 0 || ix >= peers.Count)
                                    {
                                        await self.Output.WriteAsync($"The node number must be positive and less than {peers.Count}");
                                        continue;
                                    }

                                    if (string.Equals(actions[1], "stop", StringComparison.OrdinalIgnoreCase))
                                    {
                                        await self.Output.WriteAsync($"Stopping node {ix} ({peers[ix].Item1.Key} at {peers[ix].Item1.Address}) ...");
                                        await peers[ix].Item3.RetireAsync();
                                        await self.Output.WriteAsync($"Stopped node ({peers[ix].Item1.Key} at {peers[ix].Item1.Address}) ...");
                                        //peers.RemoveAt(ix);
                                    }
                                    else if (string.Equals(actions[1], "stat", StringComparison.OrdinalIgnoreCase))
                                    {
                                        await self.Output.WriteAsync($"Requesting stats from node {ix} ({peers[ix].Item1.Key} at {peers[ix].Item1.Address}) ...");
                                        var channel = Channel.Create<PeerResponse>();
                                        await peers[ix].Item3.WriteAsync(new PeerRequest()
                                        {
                                            Operation = PeerOperation.Stats,
                                            Response = channel
                                        });

                                        await self.Output.WriteAsync($"Stats requested, waiting for response...");
                                        await self.Output.WriteAsync(System.Text.Encoding.UTF8.GetString((await channel.ReadAsync()).Data));
                                    }
                                    else if (string.Equals(actions[1], "refresh", StringComparison.OrdinalIgnoreCase))
                                    {
                                        await self.Output.WriteAsync($"Performing refresh on {ix} ({peers[ix].Item1.Key} at {peers[ix].Item1.Address}) ...");

                                        var channel = Channel.Create<PeerResponse>();
                                        await peers[ix].Item3.WriteAsync(new PeerRequest()
                                        {
                                            Operation = PeerOperation.Refresh,
                                            Response = channel
                                        });

                                        var res = await channel.ReadAsync();
                                        await self.Output.WriteAsync($"Refreshed with {res.SuccessCount} node(s)");
                                    }
                                    else
                                    {
                                        await self.Output.WriteAsync($"Node action not recognized: {actions[1]}");
                                    }
                                }
                                else
                                {
                                    await self.Output.WriteAsync($"Node command not recognized: {actions[1]}");
                                }
                            }
                            else if (string.Equals(command, "add", StringComparison.OrdinalIgnoreCase))
                            {
                                var actions = commandline.Split(new char[] { ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);
                                if (actions.Length == 1)
                                {
                                    await self.Output.WriteAsync("The add command needs the value to add");
                                    continue;
                                }
                                if (peers.Count == 0)
                                {
                                    await self.Output.WriteAsync("The add command does not work if no nodes are started");
                                    continue;
                                }

                                var channel = Channel.Create<PeerResponse>();
                                var data = System.Text.Encoding.UTF8.GetBytes(actions[1]);
                                var key = Key.ComputeKey(data);

                                await self.Output.WriteAsync($"Adding {data.Length} byte(s) with key {key}");
                                await peers[rnd.Next(0, peers.Count)].Item3.WriteAsync(new PeerRequest()
                                {
                                    Operation = PeerOperation.Add,
                                    Key = key,
                                    Data = data,
                                    Response = channel,
                                });

                                await self.Output.WriteAsync("Send add request, waiting for completion");
                                var res = await channel.ReadAsync();
                                await self.Output.WriteAsync($"Add inserted into {res.SuccessCount} node(s)");
                            }
                            else if (string.Equals(command, "get", StringComparison.OrdinalIgnoreCase))
                            {
                                var actions = commandline.Split(new char[] { ' ' }, 3, StringSplitOptions.RemoveEmptyEntries);
                                if (actions.Length == 1)
                                {
                                    await self.Output.WriteAsync("The get command needs the hash to find");
                                    continue;
                                }
                                if (actions.Length == 3)
                                {
                                    await self.Output.WriteAsync("The get command needs only one argument");
                                    continue;
                                }
                                if (peers.Count == 0)
                                {
                                    await self.Output.WriteAsync("The get command does not work if no nodes are started");
                                    continue;
                                }

                                Key key;
                                try { key = new Key(actions[1]); }
                                catch (Exception ex)
                                {
                                    await self.Output.WriteAsync($"Failed to parse key: {ex.Message}");
                                    continue;
                                }


                                var channel = Channel.Create<PeerResponse>();

                                await self.Output.WriteAsync($"Locating key");
                                await peers[rnd.Next(0, peers.Count)].Item3.WriteAsync(new PeerRequest()
                                {
                                    Operation = PeerOperation.Find,
                                    Key = key,
                                    Response = channel,
                                });

                                var res = await channel.ReadAsync();
                                if (res.Data == null)
                                    await self.Output.WriteAsync($"Did not find the key ...");
                                else
                                    await self.Output.WriteAsync($"Found: {System.Text.Encoding.UTF8.GetString(res.Data)}");
                            }
                            else if (string.Equals(command, "hash", StringComparison.OrdinalIgnoreCase))
                            {
                                var actions = commandline.Split(new char[] { ' ' }, 2, StringSplitOptions.RemoveEmptyEntries);
                                if (actions.Length == 1)
                                {
                                    await self.Output.WriteAsync("The add command needs the value to add");
                                    continue;
                                }

                                await self.Output.WriteAsync($"Key: {Key.ComputeKey(actions[1])}");
                            }
                            else
                            {
                                await self.Output.WriteAsync($"Command not recognized: {command}");
                            }
                        }
                        catch (Exception ex)
                        {
                            await self.Output.WriteAsync($"Command failed: {ex.Message}");
                        }
                    }
                }
            );

            return Task.WhenAll(consoleOut/*, consoleInput*/, proc);
        }
    }
}
