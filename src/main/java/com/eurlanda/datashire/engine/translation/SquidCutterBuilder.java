package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.entity.Squid;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
/**
 * 一把剪刀。
 * @author Gene
 *
 */
public class SquidCutterBuilder {
	private BuilderContext st;
	private List<Squid> destSquids;

	public SquidCutterBuilder(BuilderContext st) {
		this.st = st;
		if (st.debugDestinations.size() == 0)
			return; // 如果目标不为空，那么创建剪枝squid

		destSquids = new ArrayList<>();

		for (Integer destSquidId : st.debugDestinations) {
			Squid destSquid = st.getSquidById(destSquidId);
			this.destSquids.add(destSquid);
		}

	}

	/**
	 * 依据目标 squids开始剪枝。
	 */
	public List<Squid> cut() {
		if (st.debugDestinations.size() == 0)
			return st.squidFlow.getSquidList();
		LinkedList<Squid> cuttedSquids = new LinkedList<>();
		Queue<Squid> inputs = new LinkedList<>();
		inputs.addAll(this.destSquids);
		
		// 遍历 input.开始剪枝。
		while (inputs.size() > 0) { 
			Squid cur = inputs.poll();
			if (!cuttedSquids.contains(cur)) {
				cuttedSquids.addFirst(cur);
			}
			List<Squid> prevSquids = st.getPrevSquids(cur);
			if (prevSquids != null && prevSquids.size() > 0) {
				inputs.addAll(prevSquids);
			}
		}
		// 好了，剪完了，替换原始squids
		return cuttedSquids;
	}

}
