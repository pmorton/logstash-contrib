require "sequel"
require "jdbc/sqlite3"

# Tracks the visting of keys so you know what you have seen/processed
class LogStash::Util::SeenDB

  def initialize(database)
      open(database)
  end

  # Returns true if the key is present and false otherwise
  def seen?(key)
    !fetch(key).nil?
  end

  # Adds the key to the database
  def visit(key)
    @db.transaction do
      @db[:seen].where(:key => key).delete();
      @db[:seen].insert(:key => key)
    end
    nil
  end

  # Removes the key from the database
  def destroy(key)
    @db.transaction do
      @db[:seen].where(:key => key).delete();
    end
    nil
  end

  # Gets the key from the database
  def fetch(key)
    @db[:seen].where(:key => key).first
  end

  private
  def open(database)
    @db = Sequel.connect("jdbc:sqlite:#{database}")
    load_schema
  end

  private
  def load_schema
    unless @db.tables.include?(:seen)
      @db.create_table :seen do
        String   :key, :index=>true
      end
    end
  end
end
