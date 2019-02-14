require "mysql2"

File.open("tables/cities", "wb") do |f|
  client = Mysql2::Client.new(:host => "localhost", :username => "world", :database => "world")
  client.query("SELECT * FROM city").each do |row|
    fields = ["Name", "CountryCode", "District", "Population"].map do |key|
      row[key]
    end
    f.write([fields[0].bytesize, fields[0], fields[1].bytesize, fields[1], fields[2].bytesize, fields[2], fields[3]].pack("l>Z35l>Z3l>Z30q>"))
  end
end
