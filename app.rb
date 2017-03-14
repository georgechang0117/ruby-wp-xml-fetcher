require 'net/ftp'
require 'nokogiri'
require 'mysql2'
require 'logger'
require 'erb'

logger = Logger.new File.open('log/production.log', "a")
logger.info "-- Start to catch ftp xml file."
ftp_url = "input your ftp url"
ftp = Net::FTP.new(ftp_url)
logger.info "Created a new FTP session to #{ftp_url}."
ftp.login(user = "user", passwd = "passwd")
logger.info 'Ftp logined.'
dirs = ftp.ls.collect{|x| x.split(/\s+/)[8]}.delete_if {|d| d.include? "." }
#logger.info "Ftp list all directories on root directory."
if dirs.length > 0
  #puts "Sort the dirs..."
  dirs.sort!
  client = Mysql2::Client.new(:host => "localhost", :username => "username", :password => "password", :database => "database")
  logger.info "Mysql session opened."
  results = client.query("select dir_name from dirs order by updated_at limit 1")
  latest_dir_name = dirs.first
  if results.count != 0
    latest_dir_name = results.first["dir_name"]
    logger.info "Get latest dir name: #{latest_dir_name} from db."
  end
  
  dirs = dirs.reject {|d| d < latest_dir_name }
  if dirs.count > 0
    insertSQL = ""
    inserted_count = 0
    results = client.query("select file_name from files;")
    existed_files = results.map {|r| r["file_name"] }
    
    dirs.each do |dir_name|
      ftp.chdir(dir_name)
      logger.info "Ftp change directory to #{dir_name}."
      begin
        file_names = ftp.nlst("*.xml").reject {|f| existed_files.include? f }
        logger.info "Found #{file_names.length} new xml files in #{dir_name} directory."
        if file_names.length > 0
          file_names.each do |file_name|
            file_str = ftp.gettextfile(file_name ,nil)
            xml_doc = Nokogiri.XML(file_str, nil, "big5")
            articles = xml_doc.css("Article")
            articles.each do |article|
              post_title = article.children.search('HeadLine').text.gsub("'", "''")
              post_name = ERB::Util.url_encode(post_title)
             
              insertSQL = "INSERT INTO `wp_post` (`post_author`, `post_date`, `post_content`, `post_title`, `post_excerpt`, `post_status`, `comment_status`, `ping_status`, `post_password`, `post_name`, `to_ping`, `pinged`, `post_modified`, `post_content_filtered`, `post_parent`, `guid`, `menu_order`, `post_type`, `post_mime_type`, `comment_count`) VALUES (1, now(), '#{article.children.search('Segment').text.gsub("'", "''")}', '#{post_title}', '', 'draft', 'open', 'open', '', '#{post_name}', '', '', now(), '', 0, '', 0, 'post', '', 0);"
              results = client.query(insertSQL)
              post_id = client.last_id
              updateSQL = "update post_catcher set guid='http://www.url.com/?p=#{post_id}' where id=#{post_id}"
              client.query(updateSQL)
              insertSQL = "insert into wp_postmeta(post_id, meta_key, meta_value) values(#{post_id},'AuthorName', '#{article.children.search('Creator').text.gsub("'", "''")}');"
              client.query(insertSQL)
              category_name = article.children.search('Category').text.gsub("'" => "''",";" => "") 
              category_ids = client.query("select wp_term_taxonomy.term_taxonomy_id from wp_terms inner join wp_term_taxonomy on wp_terms.term_id=wp_term_taxonomy.term_id where wp_term_taxonomy.taxonomy='category' and wp_terms.name='#{category_name}' ;")
              if category_ids.count > 0
                insertSQL = "insert into wp_term_relationships(object_id, term_taxonomy_id, term_order) values(#{post_id}, #{category_ids.first["term_taxonomy_id"]},0);"
                client.query(insertSQL)
              end
              inserted_count += 1
            end
            insertSQL = "insert into files (file_name, updated_at) values('#{file_name}', now());"
            results = client.query(insertSQL)
          end
        end
        logger.info "Inserted #{inserted_count} posts from #{dir_name} dir to db."
      rescue Net::FTPPermError
        logger.error "Raised 550 No files found in #{dir_name}."
      rescue => error
        logger.error error.backtrace
        raise
      end
      latest_dir_name = dir_name
      logger.info "Updating latest dir name to #{dir_name}" 
      ftp.chdir('/')
      logger.info "Ftp return directory to root directory."
    end
    
    results = client.query("insert into post_catcher_dir(dir_name, updated_at) values ('#{latest_dir_name}', now());")
    logger.info "The lastest dir name on db is updated." 
    client.close
    logger.info "Closed Mysql session."
  end
else
  logger.info "There is no directory on ftp's root directory, close the ftp session."
end
ftp.close
logger.info "Ftp session closed."
logger.info "-- The End"
