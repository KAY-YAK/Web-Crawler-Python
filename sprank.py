import sqlite3

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# ///////////////////////////////////////////////GET ALL UNIQUE FROM_ID///////////////////////////////////////////////
# Find the ids that send out page rank - we only are interested
# in pages that have in and out links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
from_ids = list()
for row in cur:
    from_ids.append(row[0])
# /////////////////////////////////////////////// GET ALL UNIQUE FROM_ID ///////////////////////////////////////////////




# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ GET ALL UNIQUE TO_ID AND PUT FROM and TO INFO IN A LIST $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# Find the ids that receive page rank
to_ids = list()
links = list()

cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id : continue # We already have filtered out pages that link to themselves but still it can refer in odd ways which might show up during spidering
    # This is more efficient than searching if to_id is in from_ids
    # This is not needed Because we took distinct from_ids from LINKS
    #if from_id not in from_ids : continue
    #if to_id not in from_ids : continue
    links.append(row)
    if to_id not in to_ids : to_ids.append(to_id)

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ GET ALL UNIQUE TO_ID AND PUT FROM and TO INFO IN A LIST $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& CREATE PREV_RANKS DICT FOR KEEPING TRACK OF OD RANKS (OLD_RANK = NEW RANK) &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
# Get latest page ranks for strongly connected component
# Getting new_rank for all pages having outgoing links
prev_ranks = dict()
for node in from_ids:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node, ))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

sval = raw_input('How many iterations:')
many = 1
if ( len(sval) > 0 ) : many = int(sval)
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& CREATE PREV_RANKS DICT FOR KEEPING TRACK OF OLD RANKS (OLD_RANK = NEW RANK) &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&


# Sanity check
if len(prev_ranks) < 1 :
    print "Nothing to page rank.  Check data."
    quit()

# IN-MEMORY PAGE RANK
for i in range(many):
    # /////////////////////////////////CREATING NEXT RANK DICTIONARY/////////////////////////////////
    # print prev_ranks.items()[:5]
    next_ranks = dict();
    total = 0.0
    for (node, old_rank) in prev_ranks.items():
        total = total + old_rank
        next_ranks[node] = 0.0
    # print total
    # /////////////////////////////////CREATING NEXT RANK DICTIONARY/////////////////////////////////


    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< GET A NODE, SEND WEIGHTS TO ITS OUTBOUND LINKS AND UPDATE THEIR NEW RANK >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Find the number of outbound links and sent the page rank down each
    for (node, old_rank) in prev_ranks.items():
        # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ FOR CURRENT NODE FIND OUT ITS OUTBOUND LINKS AND PUT IN A GIVE_IDS LIST $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        # print node, old_rank
		# For each node we find all the outgoing links i.e. the to_id and store them in give_ids
        give_ids = list()
        for (from_id, to_id) in links:
            if from_id != node : continue
            # print '   ',from_id,to_id
            # Again an useless check
            #if to_id not in to_ids: continue
            if to_id not in from_ids : continue
            give_ids.append(to_id)
        if ( len(give_ids) < 1 ) : continue

        # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ FOR CURRENT NODE FIND OUT ITS OUTBOUND LINKS AND PUT IN A GIVE_IDS LIST $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$



        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% DIVIDE THE NODE'S OLD_RANK BY NO OF OUTBOUND LINKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		# A node divides its old_rank among each outgoing links
        amount = old_rank / len(give_ids)
        # print node, old_rank,amount, give_ids
        # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% DIVIDE THE NODE'S OLD_RANK BY NO OF OUTBOUND LINKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        # print next_ranks
        # print give_ids

        # **************************** UPDATE NEXT RANK OF ALL OUTGOING LINKS FROM CURRENT NODE ****************************
		# For a particular node[from_id i.e. page having outgoing link] we update its outgoing links
        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount
        # **************************** UPDATE NEXT RANK OF ALL OUTGOING LINKS FROM CURRENT NODE ****************************

    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< GET A NODE, SEND WEIGHTS TO ITS OUTBOUND LINKS AND UPDATE THEIR NEW RANK >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    #####################################################################################################################################
    # SUM OF NEW RANKS
    newtot = 0
    for (node, next_rank) in next_ranks.items():
        newtot = newtot + next_rank
    evap = (total - newtot) / len(next_ranks)

    # TRY without EVAP
    
    # print newtot, evap
    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtot = 0
    for (node, next_rank) in next_ranks.items():
        newtot = newtot + next_rank
    
    # Compute the per-page average change from old rank to new rank
    # As indication of convergence of the algorithm
    totdiff = 0
    for (node, old_rank) in prev_ranks.items():
        new_rank = next_ranks[node]
        diff = abs(old_rank-new_rank)
        totdiff = totdiff + diff

    avediff = totdiff / len(prev_ranks)
    print i+1, avediff

    # rotate
    prev_ranks = next_ranks

# Put the final ranks back into the database
#print next_ranks.items()[:5]
cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for (id, new_rank) in next_ranks.items() :
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
conn.commit()
cur.close()
