package com.nikhilrrao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
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
			.addStringField("timestamp").addStringField("timstamp_iso")
			.addNullableField("totalPrice", Schema.FieldType.DECIMAL).addStringField("type").build();

	static Schema deviceSchema = Schema.builder().addStringField("ID").addStringField("User").build();

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
					elementRow = Row.withSchema(eventSchema)
							.addValues(data.get(0), data.get(1), data.get(2), data.get(3), new BigDecimal(data.get(4)), data.get(5))
							.build();
				}

				// Output the Row representing the current POJO
				out.output(elementRow);
			} catch (Exception e) {
				LOG.error("there was an error casting this string to a row: " + row + "\n The error message is: " + e);
			}
		}
	}

	/** A SimpleFunction that converts Count into a printable string. */
	public static class FormatAsTextFnCount extends SimpleFunction<KV<String, Long>, String> {
		@Override
		public String apply(KV<String, Long> input) {
			return "Total Count of " + input.getKey() + ": " + input.getValue();
		}
	}

	/** A SimpleFunction that converts a sum into a printable string. */
	public static class FormatAsTextFnSum extends SimpleFunction<Double, String> {
		@Override
		public String apply(Double input) {
			return "Total Sum of ADD_TO_CART: " + input;
		}
	}

	/** A SimpleFunction that converts a Row into a printable string. */
	public static class FormatAsTextFnRow extends SimpleFunction<Row, String> {
		@Override
		public String apply(Row input) {
			return input.getValues().toString();
		}
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		

		// Process Device Data
		PCollection<Row> devices = p
				.apply(TextIO.read().from("gs://49b661f4-6504-4613-8917-c105e720a266/TapcartChallengeData/devices.csv"))
				.apply(Filter.by((String line) -> !line.isEmpty()))
				.apply(Filter.by((String line) -> !line.equals("ID,User"))).apply(ParDo.of(new CastToRow()))
				.setRowSchema(deviceSchema);

		// Process Events Data
		PCollection<Row> events = p
				.apply(TextIO.read().from("gs://49b661f4-6504-4613-8917-c105e720a266/TapcartChallengeData/events.csv"))
				.apply(Filter.by((String line) -> !line.isEmpty()))
				.apply(Filter
						.by((String line) -> !line.equals("deviceId,session,timestamp,timstamp_iso,totalPrice,type")))
				.apply(MapElements.into(TypeDescriptors.strings()).via((String line) -> line.replace("$", "")))
				.apply(ParDo.of(new CastToRow())).setRowSchema(eventSchema);

		PCollectionTuple eventsAndDevices = PCollectionTuple.of(new TupleTag<>("events"), events)
				.and(new TupleTag<>("devices"), devices);

		// Create a PCollection for Enriched Events
		PCollection<Row> EnrichedEvents = eventsAndDevices.apply(SqlTransform
				.query("SELECT * FROM events INNER JOIN devices ON events.deviceId = devices.ID"));

		EnrichedEvents.apply(MapElements.via(new FormatAsTextFnRow()))
				.apply(TextIO.write().to("gs://49b661f4-6504-4613-8917-c105e720a266/EnrichedEvents").withoutSharding());

		/*
		 * // Count all APP_OPENED and ADD_TO_CART events.apply(ParDo.of(new DoFn<Row,
		 * String>() {
		 * 
		 * @ProcessElement public void processElement(@Element Row event,
		 * OutputReceiver<String> out) { out.output(event.getString("type")); }
		 * })).apply(Count.<String>perElement()).apply(MapElements.via(new
		 * FormatAsTextFnCount())).apply( TextIO.write().to(
		 * "gs://49b661f4-6504-4613-8917-c105e720a266/submission_output").
		 * withoutSharding());
		 * 
		 * // Sum all ADD_TO_CART events.apply(ParDo.of(new DoFn<Row, Double>() {
		 * 
		 * @ProcessElement public void processElement(@Element Row event,
		 * OutputReceiver<Double> out) {
		 * out.output(event.getDecimal("totalPrice").doubleValue()); }
		 * })).apply(Sum.doublesGlobally()).apply(MapElements.via(new
		 * FormatAsTextFnSum())).apply( TextIO.write().to(
		 * "gs://49b661f4-6504-4613-8917-c105e720a266/submission_output").
		 * withoutSharding());
		 */

		p.run();
	}

}
