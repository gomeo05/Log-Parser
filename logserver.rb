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
require 'json'
require 'thread'
require 'ostruct'
require 'webrick'

$stdout.sync = true
$logger = lambda do |level, message|
  $stderr.puts "#{level}: #{message}"
end

opts = OptParser.new ARGV
opts.add_map :table
opts.add_multi :cleanup

def catchall
  begin
    yield
  rescue => e
    puts "EXCEPTION #{e}"
    e.backtrace.each { |x| puts x }
    false
  end
end

class LogServer

  def initialize(opts)
    @sqlite = LogParser::SQLite.new({ :table => 'logdata' })

    opts.option(:table).each do |table_name, log|
      table = LogParser::SQLite.new({ :table => table_name, :master => @sqlite })
      table_controller = LogParser::Controller.new LogParser::IISParser.new(table)
      table_controller.read log
    end

    sink = @sqlite

    if opts.flag? :strip_guids
      sink = LogParser::StripGUIDs.new sink
    end

    if opts.flag? :vip
      sink = LogParser::AddVip.new sink
    end

    sink = LogParser::IISParser.new sink

    @controller = LogParser::Controller.new sink

    # create an empty view
    db.execute "create view logs as select * from logdata where id < 0"

    @backlog = opts[:backlog] || 12 # an hour by default
    @backlog = @backlog.to_i

    @db_queue = Queue.new
    @read_queue = Queue.new
    @updates = opts[:cleanup]

    Dir.glob("#{opts[:logs]}/*.log.gz").sort.last(@backlog).each { |x| read x }

    Thread.new { db_queue }
    Thread.new { read_queue }

    self
  end

  def read logfile
    @read_queue.push logfile
  end

  def db
    @sqlite.db
  end

  def query query
    run_in_queue(@db_queue) { db.execute2(query) }
  end

  private

  def run_in_queue queue
    result = nil
    cv = ConditionVariable.new
    mutex = Mutex.new
    block = lambda do
      catchall do
        result = yield
      end
      mutex.synchronize do
        cv.signal
      end
    end
    mutex.synchronize do
      queue.push block
      cv.wait mutex
    end
    result
  end
  
  def db_queue
    while true do
      catchall do
        @db_queue.pop.call
      end
    end
  end

  def read_queue
    id_list = []
    logs_seen = {}

    while true do
      logfile = @read_queue.shift
      unless logs_seen[logfile]
        logs_seen[logfile] = true
        start_id = @sqlite.id
        catchall do
          @controller.read logfile
        end
        last_id = @sqlite.id - 1

        @updates.each do |query|
          catchall do
            db.execute query, [start_id, last_id]
          end
        end

        1.times do

          id = last_id
          block = lambda do
            db.execute "drop view logs"
            db.execute "create view logs as select * from logdata where id <= #{id}"
            id_list.push id
            if id_list.size > @backlog
              puts "deleting old logs..."
              db.execute "delete from logdata where id < ?", [ id_list.shift ]
            end
          end
          @db_queue.push block
        end
      else
        puts "already read #{logfile}, ignoring"
      end
    end

  end

end

class JSONServlet < WEBrick::HTTPServlet::AbstractServlet
  def do_POST req, resp
    params = JSON.parse req.body
    result = if self.respond_to? :do_JSON
      do_JSON *params
    else
      @options.first.call *params
    end
    resp.body = JSON.generate result
    resp.content_type = "application/json"
  end
end

$logserver = LogServer.new opts

webrick_opts = {}
webrick_opts[:Port] = opts[:port].to_i if opts[:port]
webrick_opts[:BindAddress] = opts[:host] if opts[:host]
webrick = WEBrick::HTTPServer.new webrick_opts

['INT', 'TERM'].each { |signal|
  trap(signal) do
    webrick.shutdown
    exit 1
  end
}

query_block = lambda do |request|
  $logserver.query request
end

logpush_block = lambda do |request|
  $logserver.read request
end

webrick.mount '/query', JSONServlet, query_block
webrick.mount '/read', JSONServlet, logpush_block

webrick.start
