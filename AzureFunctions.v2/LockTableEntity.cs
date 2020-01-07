using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace FileValidation
{
    class LockTableEntity : TableEntity
    {
        public LockTableEntity() : base() { }

        public LockTableEntity(string prefix) : base(prefix, prefix) { }

        [IgnoreProperty]
        public string Prefix
        {
            get => this.PartitionKey;
            set
            {
                this.PartitionKey = value;
                this.RowKey = value;
            }
        }

        [IgnoreProperty]
        public BatchState State { get; set; } = BatchState.Waiting;

        public string DbState
        {
            get => this.State.ToString();
            set => this.State = (BatchState)Enum.Parse(typeof(BatchState), value);
        }

        public enum BatchState
        {
            Waiting, InProgress, Done
        }

        public static async Task<LockTableEntity> GetLockRecordAsync(string filePrefix, CloudTable customerFilesTable = null, CloudStorageAccount customerFilesTableStorageAccount = null)
        {
            customerFilesTable = customerFilesTable ?? await Helpers.GetLockTableAsync(customerFilesTableStorageAccount);

            return (await customerFilesTable.ExecuteQueryAsync(
                new TableQuery<LockTableEntity>()
                    .Where(TableQuery.GenerateFilterCondition(@"PartitionKey", QueryComparisons.Equal, filePrefix))))
                .SingleOrDefault();
        }

        public static async Task UpdateAsync(string filePrefix, BatchState state, CloudTable customerFilesTable = null, CloudStorageAccount customerFilesTableStorageAccount = null)
        {
            var entity = await GetLockRecordAsync(filePrefix, customerFilesTable);
            entity.State = state;

            customerFilesTable = customerFilesTable ?? await Helpers.GetLockTableAsync(customerFilesTableStorageAccount);

            await customerFilesTable.ExecuteAsync(TableOperation.Replace(entity));
        }
    }
}
