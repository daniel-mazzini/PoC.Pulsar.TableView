using DotPulsar;
using System;
using System.Collections.Generic;
using System.Text;

namespace PoC.Pulsar.TableView.Infrastructure.Store;

public abstract record PartitionStartPosition(MessageId StartMessageId);

public sealed record StartFromCheckpoint(MessageId StartMessageId)
    : PartitionStartPosition(StartMessageId);

public sealed record StartFromEarliest(string Reason)
    : PartitionStartPosition(MessageId.Earliest);