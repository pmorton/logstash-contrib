# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "time"

# Stream events from files from a S3 bucket.
#
# Each line from each file generates an event.
# Files ending in '.gz' are handled as gzip'ed files.
class LogStash::Inputs::S3NG < LogStash::Inputs::Base
  config_name "s3ng"
  milestone 1

  include LogStash::PluginMixins::AwsConfig

  default :codec, "plain"

  # The S3 bucket to read logs from
  config :bucket, :validate => :string, :required => true

  # The prefix to search for logs under
  config :prefix, :validate => :string, :default => nil

  # The number of hours after the modified time to archive the file
  config :archive_after, :validate => :number, :default => -1

  #The S3 bucket to archive the logs to
  config :archive_to, :validate => :string, :default => nil

  #The prefix to append to prepend to the log file key
  config :archive_prefix, :validate => :string, :default => nil

  # Where to write the list of files it has seen. The default will write
  # sincedb files to some path matching "$HOME/.trackingdb*"
  config :trackingdb_path, :validate => :string, :default => nil

  # Interval to wait between to check the file list again after a run is finished.
  # Value is in seconds.
  config :interval, :validate => :number, :default => 60

  public
  def register
    require "digest/md5"

    require 'logstash/util/seen_db'
    initialize_tracking_db

    require "aws-sdk"
    require 's3io'
    aws_config = aws_options_hash
    s3 = AWS::S3.new( aws_config )
    @s3bucket = s3.buckets[@bucket]
    @archive_bucket = s3.buckets[@archive_to]

    initialize_tracking_db
  end

  def aws_service_endpoint(region)
    { :kinesis_endpoint => "s3.#{region}.amazonaws.com" }
  end

  public
  def run(queue)
    loop do
      process_new(queue)
      sleep(@interval)
    end
    finished
  end

  private
  def archive(log, archive_date)
    return false if @archive_to.nil?
    if log.last_modified < archive_date
      key = @archive_prefix.nil? ? log.key : File.join(@archive_prefix,log.key)
      @logger.debug("Archiving key", :key => log.key, :dest_key => key, :dest_bucket => @archive_to)
      log.copy_to(@archive_bucket.objects[key])
      log.delete
      @tracker.destroy(log.key)
      return true
    end
    return false
  end

  private
  def process_new(queue)
    AWS.memoize do
      archive_date = Time.new - (@archive_after * 60 * 60)
      @s3bucket.objects.with_prefix(@prefix).each do |log|
        if @tracker.seen?(log.key)
          archive(log, archive_date)
          @logger.debug("Skipping seen key #{log.key}")
          next
        end
        process_log(queue, log)
        @tracker.visit(log.key)
        archive(log, archive_date)
      end
    end
  end

  private
  def process_log(queue, object)
    read(object) do |line|
      @codec.decode(line) do |event|
        decorate(event)
        event.append( 'path' => File.join(@bucket,object.key) )
        queue << event
      end
    end
  end

  private
  def read(object, &block)
    S3io.open(object, 'r') do |io|
      if object.key.end_with?('.gz')
        reader = Zlib::GzipReader.new(io)
      else
        reader = io
      end
      reader.each_line do |line|
        yield line
      end
      reader.close
    end
  end

  private
  def initialize_tracking_db
    if @trackingdb_path.nil?
      if ENV['HOME'].nil?
        raise ArgumentError.new('No HOME or trackingdb_path set')
      end
      @trackingdb_path = File.join(ENV["HOME"], ".seendb_" + Digest::MD5.hexdigest("#{@bucket}+#{@prefix}"))
    end
    @trackingdb_path = @trackingdb_path
    @logger.debug("Tracker path #{@trackingdb_path}")
    @tracker = LogStash::Util::SeenDB.new(@trackingdb_path)
  end

end # class LogStash::Inputs::S3NG
