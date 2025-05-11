use std::collections::VecDeque;

use ractor::RpcReplyPort;

struct OffsetsActor;

struct TopicPartitionOffset {

}

enum Message {
    Interval,
    IntervalResponse(RpcReplyPort<String>),
}

struct State {
    interval: u64,
    waiting_response: VecDeque<RpcReplyPort<String>>,
}


//impl Actor for OffsetsActor {
//    async fn pre_start(
//            &self,
//            myself: ActorRef<Self::Msg>,
//            args: Self::Arguments,
//        ) -> Result<Self::State, ActorProcessingErr> {
//        
//    }
//}
