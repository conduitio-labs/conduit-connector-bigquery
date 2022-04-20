package googlesource

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
)

var (
	serviceAccount = "/home/nehagupta/Downloads/conduit-connectors-cf3466b16662.json" // replace with path to service account with permission for the project
	projectID      = "conduit-connectors"                                             //replace projectID created
	datasetID      = "conduit_test_dataset9"
	tableID        = "conduit_test_table"
)

// Initial setup required - project with service account.
func dataSetup() (err error) {

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location: "US", // See https://cloud.google.com/bigquery/docs/locations
	}

	// create dataset
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "duplicate") {
		return err
	}

	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference("gs://cloud-samples-data/bigquery/us-states/us-states.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.Schema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "post_abbr", Type: bigquery.StringFieldType},
	}
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}

	return nil
}

func cleanupDataSet() (err error) {

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)
	if err := table.Delete(ctx); err != nil {
		return err
	}

	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	if err = client.Dataset(datasetID).Delete(ctx); err != nil {
		fmt.Println("Error in delete: ", err)
		return err
	}
	return err
}

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err == nil {
		t.Errorf("expected no error, got %v", err)
	}

	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

func TestSuccessfulGet(t *testing.T) {
	// cleanupDataSet()
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer cleanupDataSet()

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = serviceAccount
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	cfg[googlebigquery.ConfigTableID] = tableID

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	for i := 0; i <= 4; i++ {
		// fmt.Println("Calling read again....")
		record, err := src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}

		value := string(record.Position)
		fmt.Printf("Record position found: %s", value)

		value = string(record.Payload.Bytes())
		fmt.Println(" :", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}

}

func TestSuccessfulGetWholeDataset(t *testing.T) {

	// cleanupDataSet()
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer cleanupDataSet()

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = serviceAccount
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	// cfg[googlebigquery.ConfigTableID] = tableID

	// cfg[googlebigquery.ConfigServiceAccount] = "/home/nehagupta/Downloads/conduit-connectors-cf3466b16662.json"
	// cfg[googlebigquery.ConfigProjectID] = "conduit-connectors"
	// cfg[googlebigquery.ConfigDatasetID] = "conduit_1"
	// // cfg[googlebigquery.ConfigTableID] = "test1"

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	for {

		record, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println(err)
			break
		}
		if err != nil {
			t.Errorf("some other game found: %v", err)
		}
		value := string(record.Position)
		fmt.Println("Record found:", value)
		value = string(record.Payload.Bytes())
		fmt.Println(":", value)
	}
	fmt.Println("Calling teardown for start...")
	err = src.Teardown(ctx)
	fmt.Println("Calling teardown...")
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}
}

func TestSuccessfulTearDown(t *testing.T) {

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = "/home/nehagupta/Downloads/conduit-connectors-cf3466b16662.json"
	cfg[googlebigquery.ConfigProjectID] = "conduit-connectors"
	cfg[googlebigquery.ConfigDatasetID] = "conduit_1"
	cfg[googlebigquery.ConfigTableID] = "test1"
	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}
}
