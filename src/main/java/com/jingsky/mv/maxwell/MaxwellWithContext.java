package com.jingsky.mv.maxwell;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class MaxwellWithContext extends Maxwell {

	public MaxwellWithContext(MaxwellContext context) throws SQLException, URISyntaxException {
		super(context);
	}

}
