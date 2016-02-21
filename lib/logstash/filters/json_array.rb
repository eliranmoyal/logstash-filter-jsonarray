# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"


# creates a json array contaings all the events by time or count.
# it will create a new event and cancel all small events
#
# The config looks like this:
# [source,ruby]
#     json_array {
#       collate {
#         periodic_flush => true          # put this if you are using the interval
#         interval => "30s"               # interval for max-time between flushing array
#         count => 100                    # number for max-events in array for the returned event  (default 1000)
#         array_field_name => "my_field"  # the name of the field which will contain all the events(default "events")
#       }
#     }

class LogStash::Filters::Collate < LogStash::Filters::Base

  config_name "json_array"

  # How many logs should be collated.
  config :count, :validate => :number, :default => 1000

  # The `interval` is the time window which how long the logs should be collated. (default `1m`)
  config :interval, :validate => :string, :default => "1m"

  # The array field name
  config :array_field_name, :validate => :string, :default => "events"

  public
  def register
    require "thread"
    require "rufus/scheduler"

    @mutex = Mutex.new
    @bulkDone = false
    @bulkEvents = Array.new
    @scheduler = Rufus::Scheduler.new
    @job = @scheduler.every @interval do
      @logger.info("Scheduler Activated")
      @mutex.synchronize{
        @bulkDone = true  
      }
    end
  end # def register

  public
  def filter(event)
    @logger.info("do collate filter")
    if event == LogStash::SHUTDOWN
      @job.trigger()
      @job.unschedule()
      @logger.info("collate filter thread shutdown.")
      return
    end

    event.cancel

    @mutex.synchronize{
      @bulkEvents.push(event.clone)
      if (@bulkEvents.length == @count)
        @bulkDone = true
        new_event = accumulate_events()
      end
      yield new_event if @bulkDone

      # reset bulkDone flag
      @bulkDone = false
    }
  end # def filter


  # Flush any pending messages.
  public
  def flush(options = {})
    new_event = nil
    if (@bulkDone && @bulkEvents.length > 0)
      @mutex.synchronize{
        new_event = accumulate_events()
      }
      # reset aggregationDone flag.
      @bulkDone = false
      return [new_event] 
    end
    []
  end # def flush


private
def accumulate_events
    new_event = LogStash::Event.new
    new_event[@array_field_name] = @bulkEvents.map{|x| filter_matched(x) ; x.to_json}
    @bulkEvents = Array.new
    new_event
end

end #
