using System.Diagnostics;

namespace DotNetKafkaLab.Tests;

public class WorkflowTests
{
    private static JoinInput Input(string stage, ActivityContext context, int sequence = 1)
        => new(DemoEvent.Order("test", sequence) with { Stage = stage }, context, 0, sequence, "dotnet-lab.branch.results");
    private static ActivityContext Context() => new(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded, isRemote: true);

    [Fact]
    public void JoinKeepsBothRealCrossTraceInputs()
    {
        var payment = Input("payment", Context()); var inventory = Input("inventory", Context());
        var pending = new FanInBuffer(); var now = DateTimeOffset.UtcNow;
        Assert.Null(pending.Add(payment, now));
        var joined = Assert.IsType<JoinedOrder>(pending.Add(inventory, now));
        Assert.Equal(2, joined.Links.Length);
        Assert.Equal(payment.CreationContext, joined.Links[0].Context);
        Assert.Equal(inventory.CreationContext, joined.Links[1].Context);
        Assert.NotEqual(joined.Links[0].Context.TraceId, joined.Links[1].Context.TraceId);
        Assert.Equal(0, pending.Count);
    }

    [Fact]
    public void RepeatedOneBranchDoesNotFabricateFanIn()
    {
        var pending = new FanInBuffer(); var input = Input("payment", Context());
        Assert.Null(pending.Add(input, DateTimeOffset.UtcNow));
        Assert.Null(pending.Add(input, DateTimeOffset.UtcNow));
        Assert.Equal(1, pending.Count);
    }

    [Fact]
    public void DifferentOrdersCannotJoinAndExpiredStateIsRemoved()
    {
        var now = DateTimeOffset.UtcNow; var pending = new FanInBuffer(ttl: TimeSpan.FromSeconds(1));
        Assert.Null(pending.Add(Input("payment", Context(), 1), now));
        Assert.Null(pending.Add(Input("inventory", Context(), 2), now));
        Assert.Equal(2, pending.Expire(now.AddSeconds(2)));
        Assert.Equal(0, pending.Count);
    }

    [Fact]
    public void IncompleteJoinStateStaysBounded()
    {
        var pending = new FanInBuffer(capacity: 1); var now = DateTimeOffset.UtcNow;
        pending.Add(Input("payment", Context(), 1), now);
        Assert.Throws<InvalidOperationException>(() => pending.Add(Input("inventory", Context(), 2), now));
    }

    [Theory]
    [InlineData(1, PaymentRoute.Result)]
    [InlineData(7, PaymentRoute.Retry)]
    [InlineData(11, PaymentRoute.DeadLetter)]
    [InlineData(77, PaymentRoute.DeadLetter)]
    public void RetryAndPermanentFailureRoutesAreDeterministic(int sequence, PaymentRoute expected)
        => Assert.Equal(expected, Routing.Payment(sequence));

    [Fact]
    public void SmokeExpectationsIncludeRetryAndDlq()
    {
        Assert.Equal(3, Routing.Retries(24)); Assert.Equal(2, Routing.DeadLetters(24));
        Assert.Equal("test-order-000007", DemoEvent.Order("test", 7).OrderId);
    }

    [Fact]
    public void StatePathsAndRateAreValidated()
    {
        Assert.Throws<ArgumentException>(() => Options.Parse(["run", "--run-id", "../../outside"]));
        Assert.Throws<ArgumentException>(() => Options.Parse(["run", "--rate", "100000"]));
        Assert.Throws<ArgumentException>(() => Options.Parse(["run", "--duration", "0"]));
    }
}
