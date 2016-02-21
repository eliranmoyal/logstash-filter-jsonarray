require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/json_array"

describe LogStash::Filters::Collate do
  

  describe "json_array when count is full" do
    config <<-CONFIG
      filter {
        json_array {
          count => 2
        }
      }
    CONFIG

    events = [
      {
        "@timestamp" => Time.iso8601("2013-01-02T00:00:00.000Z"),
        "message" => "1st message"
      },
      {
        "@timestamp" => Time.iso8601("2013-01-01T00:00:00.000Z"),
        "message" => "2nd message"
      }
    ]

    sample(events) do
      insist { subject["events"] }.is_a? Array
      insist { subject.length } == 2
      subject.each_with_index do |s,i|
        if i == 0 # first one should be the earlier message
          insist { Json.Parse(s)["message"] } == "1st message"
        end
        if i == 1 # second one should be the later message
          insist { Json.Parse(s)["message"] } == "2nd message"
        end
      end
    end
  end

  # (Ignored) Currently this case can't pass because of the case depends on the flush function of the filter in the test, 
  # there was a TODO marked in the code (spec_helper.rb, # TODO(sissel): pipeline flush needs to be implemented.), 
  # and the case wants to test the scenario which collate was triggered by a scheduler, so in this case, it needs to sleep few seconds 
  # waiting the scheduler triggered, and after the events were flushed, then the result can be checked.

  # describe "collate when interval reached" do
  #   config <<-CONFIG
  #     filter {
  #       collate {
  #         interval => "1s"
  #       }
  #     }
  #   CONFIG

  #   events = [
  #     {
  #       "@timestamp" => Time.iso8601("2013-01-02T00:00:00.000Z"),
  #       "message" => "later message"
  #     },
  #     {
  #       "@timestamp" => Time.iso8601("2013-01-01T00:00:00.000Z"),
  #       "message" => "earlier message"
  #     }
  #   ]

  #   sample(events) do
  #     sleep(2)
  #     insist { subject }.is_a? Array
  #     insist { subject.length } == 2
  #     subject.each_with_index do |s,i|
  #       if i == 0 # first one should be the earlier message
  #         insist { s["message"] } == "earlier message"
  #       end
  #       if i == 1 # second one should be the later message
  #         insist { s["message"]} == "later message"
  #       end
  #     end
  #   end
  # end
end
