package com.ctrip.hermes.kafka.perf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import kafka.consumer.ConsumerConfig;
import kafka.tools.PerfConfig;

public class ConsumerPerfConfig extends PerfConfig {

	ConsumerConfig consumerConfig;

	int numThreads;

	String topic;

	long numMessages;

	int reportingInterval;

	boolean showDetailedStats;

	SimpleDateFormat dateFormat;

	boolean hideHeader;

	public ConsumerPerfConfig(String[] args) {
		super(args);
		parserArgs(args);
	}

	private void parserArgs(String[] args) {
		ArgumentAcceptingOptionSpec<String> zkConnectOpt = parser()
		      .accepts(
		            "zookeeper",
		            "REQUIRED: The connection string for the zookeeper connection in the form host:port. "
		                  + "Multiple URLS can be given to allow fail-over.").withRequiredArg().describedAs("urls")
		      .ofType(String.class);
		ArgumentAcceptingOptionSpec<String> topicOpt = parser().accepts("topic", "REQUIRED: The topic to consume from.")
		      .withRequiredArg().describedAs("topic").ofType(String.class);
		ArgumentAcceptingOptionSpec<String> groupIdOpt = parser().accepts("group", "The group id to consume on.")
		      .withRequiredArg().describedAs("gid").defaultsTo("perf-consumer-" + new Random().nextInt(100000))
		      .ofType(String.class);
		ArgumentAcceptingOptionSpec<Integer> fetchSizeOpt = parser()
		      .accepts("fetch-size", "The amount of data to fetch in a single request.").withRequiredArg()
		      .describedAs("size").ofType(Integer.class).defaultsTo(1024 * 1024);
		OptionSpecBuilder resetBeginningOffsetOpt = parser()
		      .accepts(
		            "from-latest",
		            "If the consumer does not already have an established "
		                  + "offset to consume from, start with the latest message present in the log rather than the earliest message.");
		ArgumentAcceptingOptionSpec<Integer> socketBufferSizeOpt = parser()
		      .accepts("socket-buffer-size", "The size of the tcp RECV size.").withRequiredArg().describedAs("size")
		      .ofType(Integer.class).defaultsTo(2 * 1024 * 1024);
		ArgumentAcceptingOptionSpec<Integer> numThreadsOpt = parser().accepts("threads", "Number of processing threads.")
		      .withRequiredArg().describedAs("count").ofType(Integer.class).defaultsTo(10);
		ArgumentAcceptingOptionSpec<Integer> numFetchersOpt = parser()
		      .accepts("num-fetch-threads", "Number of fetcher threads.").withRequiredArg().describedAs("count")
		      .ofType(Integer.class).defaultsTo(1);

		OptionSet options = parser().parse(args);

		try {
			checkRequiredArgs(parser(), options, zkConnectOpt, topicOpt);
		} catch (IOException e) {
			e.printStackTrace();
		}

		Properties props = new Properties();
		props.put("group.id", options.valueOf(groupIdOpt));
		props.put("socket.receive.buffer.bytes", options.valueOf(socketBufferSizeOpt).toString());
		props.put("fetch.message.max.bytes", options.valueOf(fetchSizeOpt).toString());
		props.put("auto.offset.reset", options.has(resetBeginningOffsetOpt) ? "largest" : "smallest");
		props.put("zookeeper.connect", options.valueOf(zkConnectOpt));
		props.put("consumer.timeout.ms", "5000");
		props.put("num.consumer.fetchers", options.valueOf(numFetchersOpt).toString());
		consumerConfig = new ConsumerConfig(props);
		numThreads = options.valueOf(numThreadsOpt).intValue();
		topic = options.valueOf(topicOpt);
		numMessages = options.valueOf(numMessagesOpt()).longValue();
		reportingInterval = options.valueOf(reportingIntervalOpt()).intValue();
		showDetailedStats = options.has(showDetailedStatsOpt());
		dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt()));
		hideHeader = options.has(hideHeaderOpt());
	}

	private static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec<?>... required)
	      throws IOException {
		for (OptionSpec<?> arg : required) {
			if (!options.has(arg))
				printUsageAndDie(parser, "Missing required argument \"" + arg + "\"");
		}
	}

	private static void printUsageAndDie(OptionParser parser, String message) throws IOException {
		System.err.println(message);
		parser.printHelpOn(System.err);
		System.exit(1);
	}
}