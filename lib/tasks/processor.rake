KAFKA_CONFIG = {
  "bootstrap.servers": "localhost:9092",
  "enable.auto.commit": false,
  "auto.offset.reset": "earliest",
  "enable.partition.eof": false
}

namespace :processor do
  task :create_topics do
    `kafka-topics --create --topic=raw-page-views --zookeeper=127.0.0.1:2181 --partitions=8 --replication-factor=1`
    `kafka-topics --create --topic=page-views --zookeeper=127.0.01:2181 --partitions=8 --replication-factor=1`
  end

  task :import => :environment do
    puts 'Starting the importation'
    producer = Rdkafka::Config.new(KAFKA_CONFIG).producer
    Dir.glob('log/access/*') do |file|
      delivery_handles = []
      File.read(file).lines.each do |line|
        puts line
        handle = producer.produce(
          payload: line,
          topic: 'raw-page-views'
        )
        delivery_handles.push(handle)
      end
      puts "Produced lines in #{file}, waiting for delivery"
      delivery_handles.each(&:wait)
    end
    puts 'Imported all available logs in log/access'
  end

  task :preprocess => :environment do
    puts 'Processor starting...'
    log_line_regex = %r{^(\S+) - - \[(\S+ \+\d{4})\] "(\S+ \S+ [^"]+)" (\d{3}) (\d+|-) "(.*?)" "([^"]+)"$}
    geoip = GeoIP.new('GeoLiteCity.dat')
    producer = Rdkafka::Config.new(KAFKA_CONFIG).producer
    consumer = Rdkafka::Config.new(KAFKA_CONFIG.merge("group.id": "preprocessor")).consumer
    consumer.subscribe("raw-page-views")
    @last_tick_time = Time.now.to_i
    delivery_handles = []
    consumer.each do |message|
      log_line = message.payload.split(log_line_regex)
      city = geoip.city(log_line[1])
      next unless city

      user_agent = UserAgent.parse(log_line[7])
      next unless user_agent

      url = log_line[3].split[1]
      page_view = {
        time: log_line[2],
        ip: log_line[1],
        country: city.country_name,
        browser: user_agent.browser,
        url: url
      }
      puts page_view
      handle = producer.produce(
        key: city.country_code2,
        payload: page_view.to_json,
        topic: 'page-views'
      )
      delivery_handles.push(handle)
      now_time = Time.now.to_i
      if @last_tick_time + 5 < now_time
        puts 'Waiting for delivery and committing consumer'
        delivery_handles.each(&:wait)
        delivery_handles.clear
        consumer.commit
        sleep 1
        @last_tick_time = now_time
      end
    end
  end

  task :aggregate => :environment do
    consumer = Rdkafka::Config.new(KAFKA_CONFIG.merge("group.id": "processor")).consumer
    consumer.subscribe("page-views")
    @count = 0
    @country_counts = Hash.new(0)
    @last_tick_time = Time.now.to_i
    consumer.each do |message|
      page_view = JSON.parse(message.payload)
      @count += 1
      @country_counts[page_view['country']] += 1
      now_time = Time.now.to_i
      if @last_tick_time + 5 < now_time
        puts "#{Time.now}: Running for #{@country_counts.count} countries: #{@country_counts.map(&:first)}"
        CountryStat.update_country_counts(@country_counts)
        consumer.commit
        @count = 0
        @country_counts.clear
        @last_tick_time = now_time
      end
    end
  end
end
