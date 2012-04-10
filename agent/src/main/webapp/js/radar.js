$(function() {

  Diffa.Views.Radar = Backbone.View.extend({

    diffs: [10,20,30],

    initialize: function() {
      _.bindAll(this, "render", "foo");
      this.render();
    },

    foo: function() {
      return this.diffs;
    },

    render: function() {
      $(this.el).html(JST['radar/radar']());
      return this;
    }
  });

  $('.diffa-radar').each(function() {
    var domain = Diffa.DomainManager.get($(this).data('domain'));
    new Diffa.Views.Radar({el: $(this), model: domain.blobs});
  });

});