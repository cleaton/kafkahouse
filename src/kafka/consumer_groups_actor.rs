use ractor::{Actor, ActorProcessingErr, ActorRef};

struct ConsumerGroupsActor;

struct State {}

enum Message {

}

impl Actor for ConsumerGroupsActor {
    type Msg = Message;
    // and (optionally) internal state
    type State = State;
    // Startup initialization args
    type Arguments = ();
    
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(State {  })
    }

    async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            _message: Self::Msg,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn post_stop(
            &self,
            _myself: ActorRef<Self::Msg>,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}