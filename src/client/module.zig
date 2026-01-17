pub const client = @import("client.zig");
const api = @import("client.zig");

pub const Client = api.Client;
pub const Producer = api.Producer;
pub const Consumer = api.Consumer;
pub const ClusterConfig = api.ClusterConfig;
pub const ProducerConfig = api.ProducerConfig;
pub const ConsumerConfig = api.ConsumerConfig;
pub const ProduceResult = api.ProduceResult;
pub const Record = api.Record;
pub const OwnedRecord = api.OwnedRecord;
pub const PartitionError = api.PartitionError;
pub const Acks = api.Acks;
pub const StartPosition = api.StartPosition;
pub const Statistics = api.Statistics;
