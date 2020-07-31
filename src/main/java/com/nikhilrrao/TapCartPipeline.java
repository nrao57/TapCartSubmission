package com.nikhilrrao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TapCartPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(TapCartPipeline.class);

	/*
	 * Define Schema for Event and Device PCollection
	 */

	static Schema eventSchema = Schema.builder().addStringField("deviceId").addStringField("session")
			.addInt64Field("ts").addStringField("timstamp_iso").addNullableField("totalPrice", Schema.FieldType.DECIMAL)
			.addStringField("type").build();

	static Schema deviceSchema = Schema.builder().addStringField("ID").addStringField("Username").build();

	static class CastToRow extends DoFn<String, Row> {
		@ProcessElement
		public void processElement(@Element String row, OutputReceiver<Row> out) {
			try {
				List<String> data = new ArrayList<String>(Arrays.asList(row.split(",")));
				Row elementRow;
				if (data.size() == 2) {
					elementRow = Row.withSchema(deviceSchema).addValues(data.get(0), data.get(1)).build();
				} else {
					if (data.get(4).isEmpty()) {
						data.set(4, "0.00");
					}
					elementRow = Row.withSchema(eventSchema).addValues(data.get(0), data.get(1), new Long(data.get(2)),
							data.get(3), new BigDecimal(data.get(4)), data.get(5)).build();
				}

				// Output the Row representing the current POJO
				out.output(elementRow);
			} catch (Exception e) {
				LOG.error("there was an error casting this string to a row: " + row + "\n The error message is: " + e);
			}
		}
	}

	/** A SimpleFunction that converts Row Column into a Long */
	static class CastToLongs extends DoFn<Row, Long> {
		@ProcessElement
		public void processElement(@Element Row row, OutputReceiver<Long> out) {
			out.output((Long) row.getValues().get(0));
		}
	}

	/** A SimpleFunction that converts a Long into a printable string. */
	public static class FormatAsTextFnLong extends SimpleFunction<Long, String> {
		@Override
		public String apply(Long input) {
			return input.toString();
		}
	}

	/** A SimpleFunction that converts a Row into a printable string. */
	public static class FormatAsTextFnRow extends SimpleFunction<Row, String> {
		@Override
		public String apply(Row input) {
			return input.getValues().toString().replace("[", "").replace("]", "");
		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		// Create Unique ID for GCS folder to hold results
		String bucketName = "gs://49b661f4-6504-4613-8917-c105e720a266/";
		String folderId = UUID.randomUUID().toString();
		LOG.info("The results will be stored at this path: " + bucketName + folderId);

		// Process Device Data
		PCollection<Row> devices = p
				.apply("Read Devices Data",
						TextIO.read()
								.from("gs://49b661f4-6504-4613-8917-c105e720a266/TapcartChallengeData/devices.csv"))
				.apply("Skip Blank Rows", Filter.by((String line) -> !line.isEmpty()))
				.apply("Skip Header Rows", Filter.by((String line) -> !line.equals("ID,User")))
				.apply("Cast to Row with Schema", ParDo.of(new CastToRow())).setRowSchema(deviceSchema);

		// Process Events Data
		PCollection<Row> events = p
				.apply("Read Events Data",
						TextIO.read().from("gs://49b661f4-6504-4613-8917-c105e720a266/TapcartChallengeData/events.csv"))
				.apply("Skip Blank Rows", Filter.by((String line) -> !line.isEmpty()))
				.apply("Skip Header Rows", Filter
						.by((String line) -> !line.equals("deviceId,session,timestamp,timstamp_iso,totalPrice,type")))
				.apply("Remove $ Char",
						MapElements.into(TypeDescriptors.strings()).via((String line) -> line.replace("$", "")))
				.apply("Cast to Row with Schema", ParDo.of(new CastToRow())).setRowSchema(eventSchema);

		PCollectionTuple eventsAndDevices = PCollectionTuple.of(new TupleTag<>("events"), events)
				.and(new TupleTag<>("devices"), devices);

		// Create a PCollection for Enriched Events
		PCollection<Row> EnrichedEvents = eventsAndDevices.apply(SqlTransform
				.query("SELECT a.*, b.Username FROM events AS a LEFT JOIN devices AS b ON a.deviceId = b.ID"));

		EnrichedEvents.apply(MapElements.via(new FormatAsTextFnRow()))
				.apply(TextIO.write().to(bucketName + folderId + "/EnrichedEvents.csv").withoutSharding());

		// Calculate Average User Session Length
		PCollectionTuple tupleEvents = PCollectionTuple.of(new TupleTag<>("events"), events);

		tupleEvents.apply(SqlTransform.query("SELECT (MAX(ts)-MIN(ts))/COUNT(ts) FROM events GROUP BY session"))
				.apply(ParDo.of(new CastToLongs())).apply(Sum.longsGlobally())
				.apply(MapElements.via(new FormatAsTextFnLong()))
				.apply(TextIO.write().to(bucketName + folderId + "/Average_Session_Length.csv").withoutSharding());

		// Calculate Count of APP_OPENED
		PCollectionTuple tupleEvents2 = PCollectionTuple.of(new TupleTag<>("events"), events);

		tupleEvents2.apply(SqlTransform.query("SELECT COUNT(*) FROM events WHERE type = 'APP_OPENED'"))
				.apply(MapElements.via(new FormatAsTextFnRow()))
				.apply(TextIO.write().to(bucketName + folderId + "/COUNT_APP_OPENED.csv").withoutSharding());

		// Calculate Sum of ADD_TO_CART
		PCollectionTuple tupleEvents3 = PCollectionTuple.of(new TupleTag<>("events"), events);

		tupleEvents3.apply(SqlTransform.query("SELECT SUM(totalPrice) FROM events"))
				.apply(MapElements.via(new FormatAsTextFnRow()))
				.apply(TextIO.write().to(bucketName + folderId + "/SUM_ADD_TO_CART.csv").withoutSharding());

		p.run();
	}

}
