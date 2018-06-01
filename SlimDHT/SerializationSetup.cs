using System;
using System.Net;

namespace SlimDHT
{
    public static class SerializationSetup
    {
        public static void Setup()
        {
            LeanIPC.TypeSerializer.Default.RegisterCustomSerializer(
                typeof(PeerInfo),

                (type, item) => new Tuple<Type[], object[]>(
                    new Type[] { typeof(Key), typeof(string), typeof(int) },
                    new object[] {
                        ((PeerInfo)item).Key,
                        (((PeerInfo)item).Address as IPEndPoint)?.Address.ToString(),
                        (((PeerInfo)item).Address as IPEndPoint) == null ? 0 : (((PeerInfo)item).Address as IPEndPoint).Port,
                    }
                ),

                (types, values) => new PeerInfo(
                    values[0] as Key,
                    values[1] == null
                        ? null
                        : new IPEndPoint(IPAddress.Parse((string)values[1]), (int)values[2])
                )
            );

            LeanIPC.TypeSerializer.Default.RegisterCustomSerializer(
                typeof(Key),

                (type, item) => new Tuple<Type[], object[]>(
                    new Type[] { typeof(ulong[]) },
                    new object[] { ((Key)item).KeyData }
                ),

                (types, values) => new Key((ulong[])values[0])
            );

            // Make shorter packages by setting up fixed names for the types we serialize
            LeanIPC.TypeSerializer.Default.RegisterTypename(typeof(Protocol.Operation), nameof(Protocol.Operation));
            LeanIPC.TypeSerializer.Default.RegisterTypename(typeof(Protocol.Request), nameof(Protocol.Request));
            LeanIPC.TypeSerializer.Default.RegisterTypename(typeof(Protocol.Response), nameof(Protocol.Response));
            LeanIPC.TypeSerializer.Default.RegisterTypename(typeof(PeerInfo), nameof(PeerInfo));
            LeanIPC.TypeSerializer.Default.RegisterTypename(typeof(Key), nameof(Key));
            LeanIPC.TypeSerializer.Default.RegisterTypename(typeof(EndPoint), nameof(EndPoint));
            LeanIPC.TypeSerializer.Default.RegisterTypename(typeof(IPEndPoint), nameof(IPEndPoint));

            // Not required, as they are value types, so they are automatically decomposed
            LeanIPC.TypeSerializer.Default.RegisterSerializationAction(typeof(Protocol.Request), LeanIPC.SerializationAction.Decompose);
            LeanIPC.TypeSerializer.Default.RegisterSerializationAction(typeof(Protocol.Response), LeanIPC.SerializationAction.Decompose);

        }
    }
}
