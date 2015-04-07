package com.ctrip.hermes.broker.dal;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriority;
import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MTopicPartitionPriorityEntity;
import com.ctrip.hermes.broker.dal.hermes.OTopicDao;
import com.ctrip.hermes.broker.dal.hermes.RTopicGroupid;
import com.ctrip.hermes.broker.dal.hermes.RTopicGroupidDao;

public class StorageTest extends ComponentTestCase {}
