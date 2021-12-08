package com.jingsky.mv.maxwell.producer;

import com.jingsky.mv.maxwell.MaxwellContext;
import com.jingsky.mv.maxwell.row.RowMap;

public class NoneProducer extends AbstractProducer {

	public NoneProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.context.setPosition(r);
	}
}
