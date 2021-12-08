package com.jingsky.mv.maxwell.producer;

import com.jingsky.mv.maxwell.MaxwellContext;

public interface ProducerFactory {
	AbstractProducer createProducer(MaxwellContext context);
}
