package discover.model;

import shared.connectivity.thor.response.ColumnValuePair;
import shared.connectivity.thor.response.ResultInterface;
import shared.database.model.SQLColumn;
import shared.database.model.SQLQuery;
import shared.database.model.SQLTuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// This class models a SQL tuple (row) of a relation.
public class OverloadedTuple extends SQLTuple implements ResultInterface {
    
    SQLQuery query;  // The sqlQuery used to get this tuple
    private Collection<String> networks; 

    public OverloadedTuple() {
        super();
        this.query = null;
        this.networks = new ArrayList<>();
    }   
        
    /**
     * @param query the query to set
     */
    public void setQuery(SQLQuery query) {
        this.query = query;
    }

    /**
     * @param networks the networks to set
     */
    public void setNetworks(Collection<String> networks) {
        this.networks = networks;
    }

    /***********************************
     * Interface Implemented Functions *
     ***********************************/

	@Override
	public Collection<ColumnValuePair> getColumnValuePairs() {		
        List<ColumnValuePair> cvPairs = new ArrayList<ColumnValuePair>();
        for (SQLColumn column : this.getAttributes()) {
            cvPairs.add(new ColumnValuePair(column.getName(), this.getValueOfColumn(column).getValue()));
        }
        return cvPairs;
	}

	@Override
	public Collection<String> getNetworks() {
		return this.networks;
	}

	@Override
	public String getQuery() {
		return this.query.toPrettyQuery();
    }
    
    @Override
    public boolean hasScoreField() {
        return false;
    }  

	@Override
	public double getResultScore() {
		return 0;
	}
     
}
