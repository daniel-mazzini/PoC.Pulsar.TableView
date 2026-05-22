using System.Buffers;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Processor;
using Xunit;

namespace PoC.Pulsar.TableView.Processor.UnitTests;

public sealed class AvroMessageDeserializerTests
{
    [Fact]
    public void serialize_and_deserialize_should_round_trip_sport_message()
    {
        var serializer = new DefaultAvroSerializer<SportMessage>("SportMessage.avsc");
        var message = new SportMessage
        {
            Id = "sport-1",
            Provider = "provider-a",
            EntityCoverage = "global",
            Name = "Football",
            Version = 3,
            SportType = "team",
            ExternalEntities =
            [
                new ExternalEntity
                {
                    Id = "ext-1",
                    Provider = "provider-b",
                    EntityCoverage = "regional",
                    DefaultName = "Futbol"
                }
            ]
        };

        var bytes = serializer.Serialize(message);
        var roundTrip = serializer.Deserialize(new ReadOnlySequence<byte>(bytes));

        Assert.Equal(message.Id, roundTrip.Id);
        Assert.Equal(message.Provider, roundTrip.Provider);
        Assert.Equal(message.EntityCoverage, roundTrip.EntityCoverage);
        Assert.Equal(message.Name, roundTrip.Name);
        Assert.Equal(message.Version, roundTrip.Version);
        Assert.Equal(message.SportType, roundTrip.SportType);
        Assert.Single(roundTrip.ExternalEntities);
        Assert.Equal("ext-1", roundTrip.ExternalEntities[0].Id);
    }
}
