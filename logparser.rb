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

require 'logparser/controller'
require 'logparser/iis_parser'
require 'logparser/sqlite'
require 'logparser/add_vip'
require 'logparser/strip_guids'
require 'xero/opt_parser'

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

sqlite_opts = {}
if opts.has_option? :db
  sqlite_opts[:db] = opts[:db]
end

sqlite = LogParser::SQLite.new sqlite_opts

opts.option(:table).each do |table_name, log|
  table = LogParser::SQLite.new({ :table => table_name, :master => sqlite })
  table_controller = LogParser::Controller.new LogParser::IISParser.new(table)
  table_controller.read log
end

# stack up the sinks

sink = sqlite

if opts.flag? :strip_guids
  sink = LogParser::StripGUIDs.new sink
end

if opts.flag? :vip
  sink = LogParser::AddVip.new sink
end

sink = LogParser::IISParser.new sink

controller = LogParser::Controller.new sink

if opts.has_option? :checkpoint
  controller.setup_checkpoint opts[:checkpoint], opts.flag?(:checkpoint_init)
end

controller.read *(opts.params)

controller.checkpoint_write if opts.has_option? :checkpoint

#"select s_ip, sc_status, cs_uri_stem, count(sc_status) as hits "+
#    "from logs where sc_status not in (404, 400, 200, 302, 304) "+
#    "group by s_ip, sc_status, cs_uri_stem"

out = $stdout
if opts.has_option? :out
  out = File.open(opts[:out], "a")
end

opts[:query].each do |query|
  rescols = nil
  $logger[:info, "query: #{query}"]
  sqlite.db.execute2(query) do |row|
    if rescols.nil?
      rescols = row
      out.puts '#'+rescols.join("\t")
    else
      out.puts row.join("\t")
    end
  end
end
out.close unless out == $stdout

