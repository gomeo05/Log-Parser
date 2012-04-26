# Copyright (c) 2012 Xero Ltd
# 
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
# 
# The above copyright notice and this permission notice shall
# be included in all copies or substantial portions of the
# Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# ruby 1.9 doesn't add current directory to search path
$:.push File.dirname(__FILE__)

require 'logparser/controller'
require 'logparser/iis_parser'
require 'logparser/time_cutoff'
require 'logparser/iis_sink'
require 'logparser/null_sink'
require 'logparser/sqlite'
require 'logparser/add_vip'
require 'logparser/add_logfile'
require 'logparser/strip_guids'
require 'xero/opt_parser'
require 'uri'
require 'net/http'
require 'net/https'
require 'json'
require 'rexml/document'
require 'ostruct'
require 'socket'


$stdout.sync = true
$logger = lambda do |level, message|
  $stderr.puts "#{level}: #{message}"
end

opts = OptParser.new ARGV
opts.add_map :table
opts.add_multi :query

if opts.flag? :quiet
  old_logger = $logger
  $logger = lambda do |level, message|
    old_logger[level, message] if level == :error
  end
end

sqlite_opts = { :logfile => true }
if opts.has_option? :db
  sqlite_opts[:db] = opts[:db]
end

lock_dir = opts[:lock_dir] || '/tmp/logparser.lock'
cutoff_file = opts[:cutoff_file] || 'cutoff.txt'

Dir.mkdir lock_dir # will throw exception and die if dir already exists.

at_exit { Dir.rmdir lock_dir if File.directory? lock_dir }

runs_remaining = 1

runs_remaining = opts[:runs].to_i if opts[:runs]

nagios_cmd_file = opts[:nagios_cmd_file]
nagios_map = []
if opts[:nagios_map]
  doc = File.open(opts[:nagios_map], "r") { |f| REXML::Document.new f }
  nagios_el = doc.get_elements('/nagios').first
  nagios_el.get_elements('point').each do |el|
    hash = {}
    [ :graphite, :host, :service, :warning, :critical ].each do |field|
      hash[field] = el.attributes[field.to_s]
    end
    [ :warning, :critical ].each { |field| hash[field] = hash[field].to_i }
    nagios_map.push OpenStruct.new hash
  end
end

while runs_remaining > 0
  run_start = Time.new
  runs_remaining -= 1
  # step 1: equiv to start_run.rb
  cutoff = nil
  begin
    open(cutoff_file, "r") do |f|
      cutoff = f.gets.to_i
    end
  rescue
    $stderr.puts "unable to read cutoff file, starting a new one"
  end

  boundary = ((Time.new.to_i - 90) / 300) * 300

  if cutoff.nil?
    cutoff = boundary
  else
    cutoff += 300
  end
  
  if cutoff > boundary
    $stderr.puts "run complete"
    exit 0
  end

  utc = Time.at(cutoff-300).getutc

  time_slug = sprintf "%04d-%02d-%02d_%02d%02d", utc.year, utc.month, utc.day, utc.hour, utc.min

  output_logfile = (opts[:out] || "last5_SLUG.log.gz").sub /SLUG/, time_slug

  # step 2: equiv to last5_incremental

  sqlite = LogParser::SQLite.new sqlite_opts

  opts.option(:table).each do |table_name, log|
    table = LogParser::SQLite.new({ :table => table_name, :master => sqlite })
    table_controller = LogParser::Controller.new LogParser::IISParser.new(table)
    table_controller.read log
  end

  # stack up the processing pipeline

  sink = sqlite

  if opts.flag? :strip_guids
    sink = LogParser::StripGUIDs.new sink
  end

  if opts.flag? :vip
    sink = LogParser::AddVip.new sink
  end

  sink = LogParser::IISSink.new output_logfile, sink

  sink = LogParser::AddLogfile.new sink

  discriminator = LogParser::TimeCutoff.new(Time.at(cutoff), sink)

  sink = LogParser::IISParser.new discriminator

  controller = LogParser::Controller.new sink

  controller.setup_checkpoint (opts[:checkpoint] || "checkpoint.marshal")

  if controller.checkpoint_init
    $stderr.puts "Checkpoint init mode"
    discriminator.next = LogParser::NullSink.new
  end

  controller.read *(opts.params)

  if controller.checkpoint_init
    $stderr.puts "Init done, please feed me some new data"
    controller.checkpoint_write
    exit 0
  end


  sink.flush

  zero_fn = opts[:zero] || 'zero.txt'
  zero = []
  begin
    open(zero_fn,"r") do |f|
      f.each do |l|
        l.sub! /[\r\n]+$/, ''
        zero.push l
      end
    end
  rescue
    # ignore errors, assume zero_fn didn't exist
  end

  points = {}
  zero.each do |z|
    points[z] = 0 unless z == 'timestamp'
  end


  opts[:query].each do |query|
    rescols = nil
    $logger[:info, "query: #{query}"]
    prefix_cols = nil
    if query =~ /^([0-9]+),(.*)$/
      prefix_cols = $1.to_i
      query = $2
    end
    sqlite.db.execute2(query) do |row|
      if rescols.nil?
        rescols = row
      else
        if prefix_cols.nil?
          points[row[0]] = row[1].to_i
        else
          px0 = row[0..(prefix_cols - 1)].map do |col|
            col.gsub /\./, '_'
          end
          px0 = px0.join '.'
          prefix_cols.upto(row.length-1) do |i|
            points["#{px0}.#{rescols[i]}"] = row[i]
          end

        end
      end
    end
  end

  points.delete(nil) if points.has_key? nil
  points.delete('') if points.has_key? ''

  open(zero_fn,"w") do |f|
    points.keys.each { |x| f.puts x }
  end

  sink.flush
  sink.close
  begin

    if opts[:ping_url]
      ping_url = opts[:ping_url]
      ping_url_o = URI.parse ping_url
      ping_http = Net::HTTP.new(ping_url_o.host, ping_url_o.port)
      ping_http.post ping_url_o.path, JSON.generate([output_logfile])
    end
  rescue => e
    puts "Ping failure #{e}"
    e.backtrace.each { |x| puts x }
  end

  # post update to Graphite

  Socket.tcp(opts[:graphite_host] || "127.0.0.1", (opts[:graphite_port] || "2003").to_i) do |sock|
    points.each do |key, value|
      sock.puts "#{key} #{value} #{cutoff}"
    end
  end

  # now tell nagios
  unless nagios_cmd_file.nil?
    prefix = "[#{Time.now.to_i}] PROCESS_SERVICE_CHECK_RESULT;"
    updates = nagios_map.map do |point|
      if points.has_key? point.graphite
        value = points[point.graphite]
        state = 0 # OK
        if point.warning > point.critical
          state = 1 if value <= point.warning
          state = 2 if value <= point.critical
        else
          state = 1 if value >= point.warning
          state = 2 if value >= point.critical
        end
        state_name = ['OK','WARNING','CRITICAL'][state]
        "#{prefix}#{point.host};#{point.service};#{state};"+
          "#{state_name} #{value}|value=#{value};#{point.warning};#{point.critical}\n"
      else
        nil
      end
    end
    updates = updates.compact.join ''
    puts "nagios updates: #{updates}"
    File.open(nagios_cmd_file,'a') { |f| f.write(updates) }
  end

  # everything is OK, write out state and go around the loop again

  controller.checkpoint_write

  open(cutoff_file+".tmp", "w") do |f|
    f.puts cutoff
  end
  File.rename cutoff_file+".tmp", cutoff_file

  # post log parser statistics to graphite
  
  run_length = Time.new - run_start
  run_lines = controller.total_lines

  Socket.tcp(opts[:graphite_host] || "127.0.0.1", (opts[:graphite_port] || "2003").to_i) do |sock|
    {"logparser.runtime" => run_length, "logparser_lines" => run_lines}.each do |key, value|
      sock.puts "#{key} #{value} #{cutoff}"
    end
  end

end




