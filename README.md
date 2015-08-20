# The way to run it:
    require './query'
    Person.find(:conditions => ['last_name like ? and (first_name = ? or alias = ?)', 'smith', 'john', 'johnny'], :size => 1, :from => 10)
    
